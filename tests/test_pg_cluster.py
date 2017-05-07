from time import sleep

import pytest

DB = "demo_db"


def test_simple_replication1(cluster_context):
    cluster_context.setup()
    cluster = cluster_context.cluster
    master = cluster.master

    master.pg_execute("update person.addresstype set name='test12' "
                      "where addresstypeid=1", db=DB)
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


def test_kill_standby(cluster_context):
    """
    Action: poweroff a standby
    Action: sleep 15 seconds
    Check: crm_node -p : leaves only two nodes
    Action: execute an update
    Check: remaining slave is updated
    Action: power on standby
    Action: pcs cluster start standby
    Check: crm_node -p : has three nodes
    Check: started slave is updated
    """
    cluster_context.setup()
    cluster = cluster_context.cluster

    killed_standby = cluster.standbies[-1]
    other_standbies = cluster.standbies[:-1]
    master = cluster.master
    killed_standby.vm_poweroff()
    sleep(15)
    remaining_nodes = {master.name}
    for standby in other_standbies:
        remaining_nodes.add(standby.name)
    crm_nodes = set(master.ssh_run("crm_node -p", get_output=True).split())
    assert remaining_nodes == crm_nodes


def test_kill_master(cluster_context):
    """
    Action: poweroff standbies and immediately
    run some update SQL on master
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
    master.pg_execute(
        "update person.addresstype "
        "set name='test12' where addresstypeid=1", db=DB)
    select_sql = "select name from person.addresstype where addresstypeid=1"
    rs = master.pg_execute(select_sql, db=DB)
    assert 'test12' == rs[0][0]
    sleep(0.5)
    for standby in cluster.standbies:
        rs = standby.pg_execute(select_sql, db=DB)
        assert 'test12' == rs[0][0]
    # for standby in self.cluster.standbies:
    #     standby.vm_poweroff()
    master.pg_execute(
        "update person.addresstype "
        "set name='test123' where addresstypeid=1", db=DB)
