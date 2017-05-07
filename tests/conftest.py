import pytest
from time import sleep
import json

import deployer


class ClusterContext:
    cluster_json = """\
        {
          "ova": "../centos7_5.ova",
          "cluster_name": "TestCluster",
          "virtual_ip": "192.168.72.221",
          "users": ["root", "postgres", "repl1"],
          "pg_ra": "pgha/pgha.py",
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
