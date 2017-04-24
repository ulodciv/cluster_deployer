import json
import unittest

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


class TestDemoteMaster(unittest.TestCase):
    pass


class TestDeployer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cluster = deployer.Cluster(None, json.loads(cluster_json))
        cluster.deploy()

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def test1(self):
        self.assertTrue(True)
