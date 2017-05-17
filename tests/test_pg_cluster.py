from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from itertools import chain
from time import sleep, time
import json

import pytest

from cluster import Cluster

DB = "demo_db"


def expect_query_results(query_func, expected_results, timeout):
    start_time = time()
    while True:
        rs = query_func()
        if rs == expected_results:
            print(f"got expected query results: {rs}")
            return True
        print(f"did not get expected query results, "
              f"expected: {expected_results}, got {rs}")
        if time() - start_time > timeout:
            return False
        sleep(0.25)


def expect_online_nodes(cluster, expected_nodes, timeout):
    start_time = time()
    while True:
        l = cluster.ha_resource_slaves_masters("pgsql-ha")
        nodes = set(chain(*l))
        if nodes == expected_nodes:
            print(f"got expected nodes in cluster: {nodes}")
            return True
        print(f"did not get expected nodes in cluster, "
              f"expected: {expected_nodes}, got {nodes}")
        if time() - start_time > timeout:
            return False
        sleep(1)


def expect_master_node(cluster, expected_master, timeout):
    start_time = time()
    while True:
        masters = cluster.ha_resource_masters("pgsql-ha")
        if masters == [expected_master]:
            print(f"got expected masters in cluster: {masters}")
            return True
        print(f"did not get expected masters in cluster, "
              f"expected: {[expected_master]}, got {masters}")
        if time() - start_time > timeout:
            return False
        sleep(1)


def expect_node_in_recovery_or_not(expect_in_recovery, node, timeout):
    start_time = time()
    while True:
        in_recovery = node.pg_execute("SELECT pg_is_in_recovery()") == [['t']]
        if in_recovery == expect_in_recovery:
            print(f"got expected in_recovery: {in_recovery}")
            return True
        print(f"did not get expected in_recovery, "
              f"expected: {expect_in_recovery}, got {in_recovery}")
        if time() - start_time > timeout:
            return False
        sleep(1)


class ClusterContext:
    with open("config/tests.json") as fh:
        cluster_json = json.load(fh)

    def __init__(self):
        self.cluster = Cluster(None, self.cluster_json)
        cluster = self.cluster
        cluster.deploy()
        sleep(10)
        expect_online_nodes(cluster, {vm.name for vm in cluster.vms}, 30)
        # return
        cluster.ha_standby_all()
        sleep(10)  # let vms settle a bit before taking snapshots
        for vm in cluster.vms:
            vm.vm_pause()
        for vm in cluster.vms:
            vm.vm_take_snapshot("snapshot1")

    def setup(self):
        # return
        cluster = self.cluster
        for vm in cluster.vms:
            try:
                vm.vm_poweroff()
            except:
                pass  # machine is already powered off
        for vm in cluster.vms:
            vm.vm_restore_snapshot("snapshot1")
        sleep(5)
        cluster.master = cluster.vms[0]
        for vm in cluster.vms:
            vm.vm_start()
        sleep(2)
        for vm in cluster.vms:
            vm.wait_until_port_is_open(22, 10)
        cluster.ha_unstandby_all()
        expect_online_nodes(cluster, {vm.name for vm in cluster.vms}, 30)
        expect_master_node(cluster, cluster.master.name, 25)


@pytest.fixture(scope="session")
def cluster_context():
    context = ClusterContext()
    yield context
    return
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
    for standby in cluster.standbies:
        assert expect_query_results(
            partial(standby.pg_execute, select_sql, db=DB), [['test12']], 6)
    master.pg_execute("update person.addresstype set name='foo' "
                      "where addresstypeid=1", db=DB)
    for standby in cluster.standbies:
        assert expect_query_results(
            partial(standby.pg_execute, select_sql, db=DB), [['foo']], 6)


def test_pcs_standby(cluster_context):
    cluster_context.setup()
    cluster = cluster_context.cluster
    master = cluster.master
    standby = cluster.standbies[-1]
    cluster.ha_standby(standby)
    assert expect_online_nodes(cluster, {vm.name for vm in cluster.vms[:-1]}, 20)
    with pytest.raises(Exception):
        standby.pg_execute("select 1")
    master.pg_execute("update person.addresstype set name='test12' "
                      "where addresstypeid=1", db=DB)
    cluster.ha_unstandby(standby)
    assert expect_online_nodes(cluster, {vm.name for vm in cluster.vms}, 10)
    assert standby.pg_execute("select 1") == [['1']]
    select_sql = "select name from person.addresstype where addresstypeid=1"
    assert expect_query_results(
        partial(standby.pg_execute, select_sql, db=DB), [['test12']], 6)


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
    assert expect_online_nodes(cluster, remaining_nodes, 20)
    assert expect_master_node(cluster, master.name, 20)
    master.pg_execute(
        "update person.addresstype set name='a' where addresstypeid=1", db=DB)
    select_sql = "select name from person.addresstype where addresstypeid=1"
    for standby in other_standbies:
        assert expect_query_results(
            partial(standby.pg_execute, select_sql, db=DB), [['a']], 10)
    killed_standby.vm_start()
    sleep(15)
    cluster.ha_start_all()
    sleep(5)  # HACK: this should not be necessary
    all_nodes = {vm.name for vm in cluster.vms}
    assert expect_online_nodes(cluster, all_nodes, 25)
    sleep(1)
    assert expect_query_results(
        partial(killed_standby.pg_execute, select_sql, db=DB), [['a']], 10)


def test_trigger_switchover(cluster_context):
    cluster_context.setup()
    cluster = cluster_context.cluster
    sleep(5)
    cluster.master.ssh_run_check(
        f"crm_master -v 10000 -N {cluster.standbies[0].name} "
        f"-r {cluster.ha_pg_resource}")
    cluster.master = cluster.standbies[0]
    assert expect_master_node(cluster, cluster.master.name, 25)
    cluster.master.pg_execute(
        "update person.addresstype set name='c' where addresstypeid=1", db=DB)
    select_sql = "select name from person.addresstype where addresstypeid=1"
    for standby in cluster.standbies:
        assert expect_query_results(
            partial(standby.pg_execute, select_sql, db=DB), [['c']], 20)


def test_kill_master(cluster_context):
    """
    Action: poweroff standbies while running updates on master
    Action: poweroff master
    Action: poweron standbies
    Action: pcs cluster start standby1, standby2
    Check: the most up to date standby became Master
    TODO:
    Action: poweron Master
    Action: pcs cluster start <previous master>
    Check: replication from previous master works again
    """
    cluster_context.setup()
    cluster = cluster_context.cluster
    master = cluster.master

    if len(cluster.vms) < 3:
        print("This test requires more than two nodes")
        return

    sleep(5)  # WAL receiver timeout

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
            sleep(3)  # make last standby more up to date

    def raise_first(futures):
        for future in as_completed(futures):
            if future.exception():
                raise future.exception()

    with ThreadPoolExecutor() as e:
        raise_first([
            e.submit(run_updates_until_error),
            e.submit(poweroff_standbies)])

    master.vm_poweroff()

    for standby in cluster.standbies:
        standby.vm_start()
    sleep(15)
    for standby in cluster.standbies:
        standby.wait_until_port_is_open(22, 30)

    # cluster.master = cluster.standbies[0]
    standbies = cluster.standbies
    new_master = standbies[-1]
    killed_master = master
    remaining_standbies = standbies[:-1]
    cluster.master = new_master

    for standby in standbies:
        cluster.ha_start(standby)
    sleep(5)  # HACK: this should not be necessary
    assert expect_online_nodes(
        cluster, {vm.name for vm in standbies}, 25)
    new_master.ssh_run_check(
        f"crm_master -v 10000 -N {remaining_standbies[0].name} "
        f"-r {cluster.ha_pg_resource}")
    assert expect_master_node(cluster, new_master.name, 25)
    # killed_master.vm_start()
    # sleep(20)
    # cluster.ha_start(killed_master)
