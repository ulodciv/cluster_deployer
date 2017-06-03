from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from itertools import chain
from pathlib import PurePosixPath
from time import sleep, time
import json

import pytest

from pgha_deployer import PghaCluster

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
        l = cluster.master.ha_resource_slaves_masters(
            cluster.pgha_resource_master)
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
    if expected_master:
        expected_masters = [expected_master]
    else:
        expected_masters = []
    start_time = time()
    while True:
        masters = cluster.master.ha_resource_masters(
            cluster.pgha_resource_master)
        if masters == expected_masters:
            print(f"got expected masters in cluster: {masters}")
            return True
        print(f"did not get expected masters in cluster, "
              f"expected: {expected_masters}, got {masters}")
        if time() - start_time > timeout:
            return False
        sleep(1)


def expect_any_master_node(cluster, expected_masters, timeout):
    start_time = time()
    while True:
        masters = cluster.master.ha_resource_masters(
            cluster.pgha_resource_master)
        matches = set(masters) & set(expected_masters)
        if matches:
            print(f"got one or more expected masters in cluster: {matches}")
            return list(matches)
        print(f"did not get any expected masters in cluster, "
              f"expected: {expected_masters}, got {masters}")
        if time() - start_time > timeout:
            return []
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


def expect_pg_isready(node, timeout):
    start_time = time()
    while True:
        if node.pg_isready():
            print(f"PG is ready")
            return True
        print(f"PG is not ready")
        if time() - start_time > timeout:
            return False
        sleep(1)


def expect_standby_is_replicating(master, standby_name, timeout):
    start_time = time()
    slot = standby_name.replace('-', '_')
    while True:
        replicating = master.pg_execute(
            f"SELECT active FROM pg_replication_slots "
            f"WHERE slot_name='{slot}'") == [['t']]
        if replicating:
            print(f"{standby_name} is replicating with {master.name}")
            return True
        print(f"{standby_name} is not replicating with {master.name}")
        if time() - start_time > timeout:
            return False
        sleep(1)


def expect_nodes_have_positive_scores(cluster, vms, timeout):
    start_time = time()
    while True:
        ready = []
        for vm in vms:
            s = vm.ssh_run_check(
                f"crm_master -r {cluster.pgha_resource}", get_output=True)
            if s:
                # scope=nodes  name=master-pg value=998
                parts = [p.partition("=") for p in s.split()]
                d = {p[0]: p[2] for p in parts}
                if int(d.get("value", "0")) > 0:
                    ready.append(vm.name)
        if len(ready) == len(vms):
            print(f"all nodes {ready} have positive scores ")
            return True
        print(f"not all nodes have positive scores: "
              f"expected {[v.name for v in vms]}, got {ready}")
        if time() - start_time > timeout:
            return False
        sleep(1)


class ClusterContext:
    def __init__(self, test_cluster):
        self.cluster = PghaCluster(cluster_def=test_cluster)
        cluster = self.cluster
        cluster.deploy()
        expect_online_nodes(cluster, {vm.name for vm in cluster.vms}, 30)
        # return
        cluster.master.ha_standby_all()
        expect_online_nodes(cluster, set(), 30)
        sleep(1)  # let vms settle a bit before taking snapshots
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
        sleep(2)
        cluster.master = cluster.vms[0]
        master = cluster.master
        for vm in cluster.vms:
            vm.vm_start()
        for vm in cluster.vms:
            vm.wait_until_port_is_open(22, 10)
        master.ha_unstandby_all()
        expect_online_nodes(cluster, {vm.name for vm in cluster.vms}, 30)
        expect_master_node(cluster, master.name, 25)
        expect_nodes_have_positive_scores(cluster, cluster.vms, 30)
        for standby in cluster.standbies:
            assert expect_standby_is_replicating(master, standby.name, 30)


with open("tests/tests.json") as fh:
    clusters_json = json.load(fh)
    # del clusters_json["CentosPgSources"]


@pytest.fixture(scope="session", params=list(clusters_json.keys()))
def cluster_context(request):
    context = ClusterContext(clusters_json[request.param])
    yield context
    # return
    for vm in context.cluster.vms:
        try:
            vm.vm_poweroff()
        except Exception as e:
            print(f"exception during power off:\n{e}")
        sleep(2)  # seems like this is necessary...
        try:
            vm.vm_delete()
        except Exception as e:
            print(f"exception during delete:\n{e}")


def test_replication(cluster_context: ClusterContext):
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
            partial(standby.pg_execute, select_sql, db=DB), [['test12']], 3)
    master.pg_execute("update person.addresstype set name='foo' "
                      "where addresstypeid=1", db=DB)
    for standby in cluster.standbies:
        assert expect_query_results(
            partial(standby.pg_execute, select_sql, db=DB), [['foo']], 3)


def test_pcs_standby(cluster_context: ClusterContext):
    cluster_context.setup()
    cluster = cluster_context.cluster
    master = cluster.master
    standby = cluster.standbies[-1]
    master.ha_standby(standby)
    assert expect_online_nodes(
        cluster, {vm.name for vm in cluster.vms[:-1]}, 20)
    with pytest.raises(Exception):
        standby.pg_execute("select 1")
    master.pg_execute("update person.addresstype set name='y' "
                      "where addresstypeid=1", db=DB)
    select_sql = "select name from person.addresstype where addresstypeid=1"
    for stdby in cluster.standbies[:-1]:
        assert expect_query_results(
            partial(stdby.pg_execute, select_sql, db=DB), [['y']], 3)
    master.ha_unstandby(standby)
    assert expect_online_nodes(cluster, {vm.name for vm in cluster.vms}, 15)
    assert standby.pg_execute("select 1") == [['1']]
    select_sql = "select name from person.addresstype where addresstypeid=1"
    assert expect_query_results(
        partial(standby.pg_execute, select_sql, db=DB), [['y']], 6)


def test_trigger_switchover(cluster_context: ClusterContext):
    cluster_context.setup()
    cluster = cluster_context.cluster
    sleep(5)  # so that we change the score 5s after the last monitor
              # and 5s before the next monitor
    cluster.master.ssh_run_check(
        f"crm_master -v 10000 -N {cluster.standbies[0].name} "
        f"-r {cluster.pgha_resource}")
    cluster.master = cluster.standbies[0]
    assert expect_master_node(cluster, cluster.master.name, 25)
    for standby in cluster.standbies:
        assert expect_standby_is_replicating(
            cluster.master, standby.name, 30)
    cluster.master.pg_execute(
        "update person.addresstype set name='c' where addresstypeid=1", db=DB)
    select_sql = "select name from person.addresstype where addresstypeid=1"
    for standby in cluster.standbies:
        assert expect_query_results(
            partial(standby.pg_execute, select_sql, db=DB), [['c']], 20)


def test_kill_standby_machine(cluster_context: ClusterContext):
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
    master.ha_start_all()
    sleep(5)  # HACK: this should not be necessary
    assert expect_online_nodes(cluster, {vm.name for vm in cluster.vms}, 25)
    sleep(1)
    assert expect_query_results(
        partial(killed_standby.pg_execute, select_sql, db=DB), [['a']], 10)


def test_kill_master_machine(cluster_context: ClusterContext):
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
    sleep(8)
    for standby in cluster.standbies:
        standby.wait_until_port_is_open(22, 30)

    standbies = cluster.standbies
    new_master = standbies[-1]
    killed_master = master
    remaining_standbies = standbies[:-1]
    cluster.master = new_master

    for standby in standbies:
        new_master.ha_start(standby)
    sleep(3)  # HACK: this should not be necessary
    assert expect_online_nodes(
        cluster, {vm.name for vm in standbies}, 25)
    new_master.ssh_run_check(
        f"crm_master -v 10000 -N {remaining_standbies[0].name} "
        f"-r {cluster.pgha_resource}")
    assert expect_master_node(cluster, new_master.name, 25)
    # killed_master.vm_start()
    # sleep(20)
    # cluster.ha_start(killed_master)


def test_kill_slave_pg(cluster_context: ClusterContext):
    cluster_context.setup()
    cluster = cluster_context.cluster
    master = cluster.master
    killed = cluster.standbies[-1]
    other_standbies = cluster.standbies[:-1]

    sleep(5)

    # send TERM to server process
    killed.ssh_run_check(f"kill {killed.pg_get_server_pid()}")
    assert not killed.pg_isready()
    master.pg_execute(
        "update person.addresstype set name='xx' where addresstypeid=1", db=DB)
    select_sql = "select name from person.addresstype where addresstypeid=1"
    for standby in other_standbies:
        assert expect_query_results(
            partial(standby.pg_execute, select_sql, db=DB), [['xx']], 10)
    assert expect_pg_isready(killed, 30)
    assert expect_query_results(
        partial(killed.pg_execute, select_sql, db=DB), [['xx']], 10)

    sleep(2)  # action start might not be done yet

    # send KILL to server process
    killed.ssh_run_check(f"kill -9 {killed.pg_get_server_pid()}")
    assert not killed.pg_isready()
    master.pg_execute(
        "update person.addresstype set name='yy' where addresstypeid=1", db=DB)
    select_sql = "select name from person.addresstype where addresstypeid=1"
    for standby in other_standbies:
        assert expect_query_results(
            partial(standby.pg_execute, select_sql, db=DB), [['yy']], 10)
    assert expect_pg_isready(killed, 30)
    assert expect_query_results(
        partial(killed.pg_execute, select_sql, db=DB), [['yy']], 10)


def test_kill_master_pg(cluster_context: ClusterContext):
    cluster_context.setup()
    cluster = cluster_context.cluster
    master = cluster.master

    # send TERM to server process
    master.ssh_run_check(f"kill {master.pg_get_server_pid()}")
    assert expect_master_node(cluster, None, 30)
    assert expect_master_node(cluster, master.name, 30)
    assert expect_pg_isready(master, 30)
    assert expect_node_in_recovery_or_not(False, master, 10)
    for standby in cluster.standbies:
        assert expect_standby_is_replicating(cluster.master, standby.name, 30)
    master.pg_execute(
        "update person.addresstype set name='xx' where addresstypeid=1", db=DB)
    select_sql = "select name from person.addresstype where addresstypeid=1"
    for standby in cluster.standbies:
        assert expect_query_results(
            partial(standby.pg_execute, select_sql, db=DB), [['xx']], 10)

    # send KILL to server process
    master.ssh_run_check(f"kill -9 {master.pg_get_server_pid()}")
    assert expect_master_node(cluster, None, 30)
    assert expect_master_node(cluster, master.name, 30)
    assert expect_pg_isready(master, 30)
    assert expect_node_in_recovery_or_not(False, master, 10)
    for standby in cluster.standbies:
        assert expect_standby_is_replicating(cluster.master, standby.name, 30)
    master.pg_execute(
        "update person.addresstype set name='yy' where addresstypeid=1", db=DB)
    select_sql = "select name from person.addresstype where addresstypeid=1"
    for standby in cluster.standbies:
        assert expect_query_results(
            partial(standby.pg_execute, select_sql, db=DB), [['yy']], 10)


def test_cluster_stop_start(cluster_context: ClusterContext):
    cluster_context.setup()
    cluster = cluster_context.cluster
    master = cluster.master
    master.ha_stop_all()
    for vm in cluster.vms:
        with pytest.raises(Exception):
            vm.pg_execute("select 1")
    master.ha_start_all()
    assert expect_online_nodes(cluster, {vm.name for vm in cluster.vms}, 25)
    master.ssh_run_check(
        f"crm_master -v 10000 -N {master.name} "
        f"-r {cluster.pgha_resource}")
    assert expect_master_node(cluster, master.name, 5)
    for vm in cluster.vms:
        assert expect_pg_isready(vm, 10)
    master.pg_execute(
        "update person.addresstype set name='ee' where addresstypeid=1", db=DB)
    select_sql = "select name from person.addresstype where addresstypeid=1"
    for standby in cluster.standbies:
        assert expect_query_results(
            partial(standby.pg_execute, select_sql, db=DB), [['ee']], 25)


def test_break_pg_on_master(cluster_context: ClusterContext):
    """ check that
        - another standby becomes master
        - broken master is taken offline """
    cluster_context.setup()
    cluster = cluster_context.cluster
    master = cluster.master
    # sleep(20)  # long enough for scores to be set
    master.ssh_run_check(f"kill -9 {master.pg_get_server_pid()}")
    if master.pg_version == "10":
        pg_xlog_dir = PurePosixPath(master.pg_data_directory) / "pg_wal"
    else:
        pg_xlog_dir = PurePosixPath(master.pg_data_directory) / "pg_xlog"
    master.ssh_run_check(f"mv {pg_xlog_dir} {pg_xlog_dir}.bak")
    assert expect_master_node(cluster, None, 30)
    masters = expect_any_master_node(
        cluster, [v.name for v in cluster.standbies], 30)
    assert len(masters) == 1
    new_master = [v for v in cluster.standbies if v.name == masters[0]][0]
    remaining_standbies = [v for v in cluster.standbies if v != new_master]
    cluster.master = new_master
    for standby in remaining_standbies:
        assert expect_standby_is_replicating(cluster.master, standby.name, 30)
