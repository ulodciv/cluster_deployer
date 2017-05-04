import json
import unittest
from time import sleep

import deployer


cluster_json = """\
{
  "ova": "../../centos7_5.ova",
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
}
"""


class TestDeployer(unittest.TestCase):
    DB = "demo_db"

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.cluster = deployer.Cluster(None, json.loads(cluster_json))
        self.cluster.deploy()
        sleep(40)

    # def tearDown(self):
    #     for vm in self.cluster.vms:
    #         try:
    #             print(vm.vm_poweroff())
    #         except Exception as e:
    #             print(f"exception during power off:\n{e}")
    #         sleep(2)  # seems like this is necessary...
    #         try:
    #             print(vm.vm_delete())
    #         except Exception as e:
    #             print(f"exception during delete:\n{e}")

    def test_simple_replication(self):
        master = self.cluster.master

        master.pg_execute(
            "update person.addresstype "
            "set name='test12' where addresstypeid=1", db=self.DB)
        select_sql = "select name from person.addresstype where addresstypeid=1"
        rs = master.pg_execute(select_sql, db=self.DB)
        self.assertEqual('test12', rs[0][0])
        sleep(0.5)
        for standby in self.cluster.standbies:
            rs = standby.pg_execute(select_sql, db=self.DB)
            self.assertEqual('test12', rs[0][0])

        master.pg_execute(
            "update person.addresstype "
            "set name='foo' where addresstypeid=1", db=self.DB)
        sleep(0.5)
        for standby in self.cluster.standbies:
            rs = standby.pg_execute(select_sql, db=self.DB)
            self.assertEqual('foo', rs[0][0])

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
            "set name='test12' where addresstypeid=1", db=self.DB)
        select_sql = "select name from person.addresstype where addresstypeid=1"
        rs = master.pg_execute(select_sql, db=self.DB)
        self.assertEqual('test12', rs[0][0])
        sleep(0.5)
        for standby in self.cluster.standbies:
            rs = standby.pg_execute(select_sql, db=self.DB)
            self.assertEqual('test12', rs[0][0])
        # for standby in self.cluster.standbies:
        #     standby.vm_poweroff()
        master.pg_execute(
            "update person.addresstype "
            "set name='test123' where addresstypeid=1", db=self.db)