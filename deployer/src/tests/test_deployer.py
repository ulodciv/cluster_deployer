import json
import unittest
from time import sleep

import deployer


class ClusterContext:
    cluster_json = """\
        {
          "ova": "D:/ovas/centos7_3.ova",
          "cluster_name": "TestCluster",
          "virtual_ip": "192.168.72.221",
          "users": ["root", "postgres", "repl1"],
          "pg_ra": "../ra/pgsqlha.py",
          "pg_user": "postgres",
          "pg_repl_user": "repl1",
          "root_password": "root",
          "key_file": "key/rsa_private.key",
          "pub_key_file": "key/rsa_pub.key",
          "demo_db": "demo_db.xz",
          "hosts": [
            {
              "name": "test-pg-1",
              "static_ip": "192.168.72.151"
            },
            {
              "name": "test-pg-2",
              "static_ip": "192.168.72.152"
            },
            {
              "name": "test-pg-3",
              "static_ip": "192.168.72.153"
            }
          ]
        }"""

    def __init__(self):
        self.cluster = deployer.Cluster(None, json.loads(self.cluster_json))
        self.cluster.deploy()
        sleep(40)
        self.cluster.ha_standby_all()
        sleep(10)  # let vms settle a bit before taking snapshots
        # pause vms
        for vm in self.cluster.vms:
            vm.vm_pause()
        # take snapshot of vms
        for vm in self.cluster.vms:
            vm.vm_take_snapshot("snapshot1")

    def setup(self):
        # power off vm
        for vm in self.cluster.vms:
            try:
                vm.vm_poweroff()
            except:
                pass  # machine is already powered off
        # restore snapshot
        for vm in self.cluster.vms:
            vm.vm_restore_snapshot("snapshot1")
        sleep(3)
        # start the vm
        for vm in self.cluster.vms:
            vm.vm_start()
        self.cluster.ha_unstandby_all()
        sleep(30)

    def __del__(self):
        for vm in self.cluster.vms:
            try:
                print(vm.vm_poweroff())
            except Exception as e:
                print(f"exception during power off:\n{e}")
            sleep(2)  # seems like this is necessary...
            try:
                print(vm.vm_delete())
            except Exception as e:
                print(f"exception during delete:\n{e}")


_cluster_context = None
DB = "demo_db"


def setUpModule():
    global _cluster_context
    _cluster_context = ClusterContext()


def tearDownModule():
    global _cluster_context
    if _cluster_context is not None:
        _cluster_context = None


class TestDeployer1(unittest.TestCase):

    def setUp(self):
        _cluster_context.setup()
        self.cluster = _cluster_context.cluster

    def test_simple_replication1(self):
        master = self.cluster.master

        master.pg_execute(
            "update person.addresstype "
            "set name='test12' where addresstypeid=1", db=DB)
        select_sql = "select name from person.addresstype where addresstypeid=1"
        rs = master.pg_execute(select_sql, db=DB)
        self.assertEqual('test12', rs[0][0])
        sleep(0.5)
        for standby in self.cluster.standbies:
            rs = standby.pg_execute(select_sql, db=DB)
            self.assertEqual('test12', rs[0][0])

        master.pg_execute(
            "update person.addresstype "
            "set name='foo' where addresstypeid=1", db=DB)
        sleep(0.5)
        for standby in self.cluster.standbies:
            rs = standby.pg_execute(select_sql, db=DB)
            self.assertEqual('foo', rs[0][0])

    def test_simple_replication2(self):
        master = self.cluster.master

        master.pg_execute(
            "update person.addresstype "
            "set name='test12' where addresstypeid=1", db=DB)
        select_sql = "select name from person.addresstype where addresstypeid=1"
        rs = master.pg_execute(select_sql, db=DB)
        self.assertEqual('test12', rs[0][0])
        sleep(0.5)
        for standby in self.cluster.standbies:
            rs = standby.pg_execute(select_sql, db=DB)
            self.assertEqual('test12', rs[0][0])

        master.pg_execute(
            "update person.addresstype "
            "set name='foo' where addresstypeid=1", db=DB)
        sleep(0.5)
        for standby in self.cluster.standbies:
            rs = standby.pg_execute(select_sql, db=DB)
            self.assertEqual('foo', rs[0][0])


class TestDeployer2(unittest.TestCase):

    def setUp(self):
        _cluster_context.setup()
        self.cluster = _cluster_context.cluster

    @unittest.skip("WIP")
    def test_kill_master(self):
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
        db = "demo_db"
        master = self.cluster.master
        master.pg_execute(
            "update person.addresstype "
            "set name='test12' where addresstypeid=1", db=DB)
        select_sql = "select name from person.addresstype where addresstypeid=1"
        rs = master.pg_execute(select_sql, db=DB)
        self.assertEqual('test12', rs[0][0])
        sleep(0.5)
        for standby in self.cluster.standbies:
            rs = standby.pg_execute(select_sql, db=DB)
            self.assertEqual('test12', rs[0][0])
        # for standby in self.cluster.standbies:
        #     standby.vm_poweroff()
        master.pg_execute(
            "update person.addresstype "
            "set name='test123' where addresstypeid=1", db=db)
