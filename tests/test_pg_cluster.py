from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from time import sleep, time
import json

import pytest

from cluster import Cluster

DB = "demo_db"


class ClusterContext:
    with open("config/tests.json") as fh:
        cluster_json = json.load(fh)

    def __init__(self):
        self.cluster = Cluster(None, self.cluster_json)
        self.cluster.deploy()
        sleep(40)
        # return
        self.cluster.ha_standby_all()
        sleep(10)  # let vms settle a bit before taking snapshots
        # pause vms
        for vm in self.cluster.vms:
            vm.vm_pause()
        # take snapshot of vms
        for vm in self.cluster.vms:
            vm.vm_take_snapshot("snapshot1")

    def setup(self):
        # return
        # power off vm
        for vm in self.cluster.vms:
            try:
                vm.vm_poweroff()
            except:
                pass  # machine is already powered off
        # restore snapshot
        for vm in self.cluster.vms:
            vm.vm_restore_snapshot("snapshot1")
        sleep(5)
        # start the vm
        for vm in self.cluster.vms:
            vm.vm_start()
        sleep(3)
        self.cluster.ha_unstandby_all()
        sleep(35)


@pytest.fixture(scope="session")
def cluster_context():
    context = ClusterContext()
    yield context
    # return
    for vm in context.cluster.vms:
        try:
            print(vm.vm_poweroff())
        except Exception as e:
            print(f"exception during power off:\n{e}")
        sleep(2)  # seems like this is necessary...
        try:
            print(vm.vm_delete())
        except Exception as e:
            print(f"exception during delete:\n{e}")


def test_simple_replication1(cluster_context):
    cluster_context.setup()
    cluster = cluster_context.cluster
    master = cluster.master

    master.pg_execute(
        "update person.addresstype set name='test12' where addresstypeid=1",
        db=DB)
    select_sql = "select name from person.addresstype where addresstypeid=1"
    rs = master.pg_execute(select_sql, db=DB)
    assert 'test12' == rs[0][0]
    sleep(0.5)
    for standby in cluster.standbies:
        rs = standby.pg_execute(select_sql, db=DB)
        assert 'test12' == rs[0][0]

    master.pg_execute("update person.addresstype set name='foo' "
                      "where addresstypeid=1", db=DB)
    sleep(0.5)
    for standby in cluster.standbies:
        rs = standby.pg_execute(select_sql, db=DB)
        assert 'foo' == rs[0][0]


def test_pcs_standby(cluster_context):
    cluster_context.setup()
    cluster = cluster_context.cluster

    master = cluster.master
    standby = cluster.standbies[-1]
    cluster.ha_standby(standby)
    sleep(5)

    with pytest.raises(Exception):
        standby.pg_execute("select 1")
    master.pg_execute("update person.addresstype set name='test12' "
                      "where addresstypeid=1", db=DB)
    cluster.ha_unstandby(standby)
    sleep(5)
    assert standby.pg_execute("select 1") == [['1']]
    select_sql = "select name from person.addresstype where addresstypeid=1"
    rs = standby.pg_execute(select_sql, db=DB)
    assert 'test12' == rs[0][0]


def pg_execute_timeout(query_func, result, timeout):
    start_time = time()
    while True:
        if query_func() == result:
            return True
        if time() - start_time > timeout:
            return False
        sleep(0.25)


def expect_nodes_timeout(vm, expected_nodes, timeout):
    start_time = time()
    while True:
        nodes = set(vm.ssh_run("crm_node -p", get_output=True).split())
        print("nodes in cluster: " + str(nodes))
        if nodes == expected_nodes:
            return True
        if time() - start_time > timeout:
            return False
        sleep(1)


def test_kill_standby(cluster_context):
    """
    Action: poweroff a standby
    Action: sleep 15 seconds
    Check: crm_node -p: excludes powered off node
    Action: execute an update
    Check: remaining slaves are updated
    Action: power on standby
    Action: pcs cluster start standby
    Check: crm_node -p: has all nodes
    Check: started slave is updated
    """
    cluster_context.setup()
    cluster = cluster_context.cluster
    master = cluster.master

    killed_standby = cluster.standbies[-1]
    other_standbies = cluster.standbies[:-1]
    killed_standby.vm_poweroff()
    remaining_nodes = {master.name}
    for standby in other_standbies:
        remaining_nodes.add(standby.name)

    assert expect_nodes_timeout(master, remaining_nodes, 20), \
        "Standby takes too long to be removed from cluster"

    master.pg_execute(
        "update person.addresstype set name='a' where addresstypeid=1", db=DB)
    select_sql = "select name from person.addresstype where addresstypeid=1"
    for standby in other_standbies:
        assert pg_execute_timeout(partial(standby.pg_execute, select_sql, db=DB),
                                  [['a']], 3)
        # rs = standby.pg_execute(select_sql, db=DB)
        # assert 'a' == rs[0][0]
    killed_standby.vm_start()
    sleep(15)
    cluster.ha_start_all()
    all_nodes = {vm.name for vm in cluster.vms}
    assert expect_nodes_timeout(master, all_nodes, 25), \
        "Standby takes too long to be back in cluster"
    assert pg_execute_timeout(partial(standby.pg_execute, select_sql, db=DB),
                              [['a']], 3)


def test_kill_master(cluster_context):
    """
    Action: poweroff standbies while running updates on master
    Action: poweroff master
    Action: poweron standbies
    Action: pcs cluster start standby1, standby2
    Check: a standby became Master
    Action: poweron Master
    Action: pcs cluster start <previous master>
    Check: replication from previous master works again
    """
    cluster_context.setup()
    cluster = cluster_context.cluster
    master = cluster.master

    def run_updates_until_error():
        try:
            for i in range(10000):
                master.pg_execute(
                    "update person.addresstype "
                    "set name='foo{}' where addresstypeid=1".format(i), db=DB)
        except:
            pass

    def poweroff_standbies():
        for stdby in cluster.standbies:
            stdby.vm_poweroff()

    def raise_first(futures):
        for future in as_completed(futures):
            if future.exception():
                raise future.exception()

    with ThreadPoolExecutor() as e:
        raise_first([
            e.submit(run_updates_until_error), e.submit(poweroff_standbies)])

    master.vm_poweroff()

    for standby in cluster.standbies:
        standby.vm_start()

    sleep(20)
    # cluster.master = cluster.standbies[0]
    to_start = " ".join(vm.name for vm in cluster.standbies)
    cluster.standbies[0].ssh_run_check(f"pcs cluster start {to_start}")
    sleep(20)
    cluster.standbies[0].ssh_run_check(f"crm_resource --cleanup")  # HACK
    master.vm_start()
    sleep(20)
    cluster.ha_start(master)
