import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from os.path import join, expanduser
from time import time

from ha import HA
from postgres import Postgres
from vm import Vbox

HOME_DIR = expanduser("~")
IP_PREFIX = "192.168.72"
NAME_PREFIX = "centos-pg-"
BASE_HOST = {
    "vm_ova": join(HOME_DIR, r"dev\centos7_2.ova"),
    # "vm_ova": join(HOME_DIR, r"dev\debian9_1.ova"),
    "cluster": "PostgresCluster",
    "cluster_vip": f"{IP_PREFIX}.201",
    "root_password": "root",
    "users": ["root", "postgres", "user1", "repl1"],
    "pg_replication_user": "repl1",
    "pg_root_user": "postgres",
}
HOSTS = [
    {
        **BASE_HOST,
        "name": f"{NAME_PREFIX}1",
        "static_ip": f"{IP_PREFIX}.101",
    },
    {
        **BASE_HOST,
        "name": f"{NAME_PREFIX}2",
        "static_ip": f"{IP_PREFIX}.102",
    },
    {
        **BASE_HOST,
        "name": f"{NAME_PREFIX}3",
        "static_ip": f"{IP_PREFIX}.103",
    }
]
HOST1 = {
    **BASE_HOST,
    "name": "pg02",
    "static_ip": f"192.168.72.102",
}


class Deployer(Vbox, Postgres, HA):
    def __init__(self, **kwargs):
        super(Deployer, self).__init__(**kwargs)

    def deploy_and_setup(self):
        self.vm_deploy(False)
        self.vm_start_and_get_ip(False)
        self.setup_users()
        self.set_hostname()
        self.set_static_ip()

    def deploy_demo_db(self, db):
        self.pg_create_db("demo_db")
        self.ssh_run_check(
            [f"tar -xf {db}.xz",
             f"mv {db} /tmp/",
             f"chown -R {self.pg_root_user}. /tmp/{db}"])
        self.ssh_run_check(
            [f'cd /tmp/{db} && psql -d {db} < /tmp/{db}/install.sql',
             f'rm -rf /tmp/{db}'],
            self.pg_root_user)

    def deploy_ha(self, vms):
        self.ha_base_setup(vms)
        self.ha_set_migration_threshold(5)
        self.ha_set_resource_stickiness(10)
        self.ha_disable_stonith()
        self.ha_export_xml()
        self.ha_add_pg_to_xml()
        self.ha_add_pg_vip_to_xml()
        self.ha_import_xml()
        # self.ha_create_vip()


def raise_first(futures):
    for future in as_completed(futures):
        if future.exception():
            raise future.exception()


def deploy_cluster():
    vms = [Deployer(**host) for host in HOSTS]
    with ThreadPoolExecutor() as e:
        raise_first([e.submit(v.deploy_and_setup) for v in vms])
        raise_first([e.submit(v.add_hosts_to_etc_hosts, vms) for v in vms])
        raise_first([e.submit(v.put_ssh_key_on_others, vms) for v in vms])
        raise_first([e.submit(v.pg_create_wal_dir) for v in vms])
    master = vms[0]
    master.pg_start()
    master.deploy_demo_db("demo_db")
    master.pg_create_replication_user()
    master.pg_make_master(vms)
    master.pg_restart()
    # master.pg_add_replication_slots(vms)
    master.pg_write_recovery_for_pcmk()
    master.add_temp_ipv4_to_iface(master.ha_get_vip_ipv4())
    standbies = vms[1:]
    with ThreadPoolExecutor() as e:
        raise_first([e.submit(s.pg_standby_backup_from_master, master)
                     for s in standbies])
        raise_first([e.submit(s.pg_start) for s in standbies])
        raise_first([e.submit(vm.pg_stop) for vm in vms])
    master.del_temp_ipv4_to_iface(master.ha_get_vip_ipv4())
    master.deploy_ha(vms)


def main():
    start = time()
    deploy_cluster()
    print(f"took {timedelta(seconds=time() - start)}")
    # vm1 = MyVm(**HOSTS[1])
    # vm1.assume_ip_is_static()
    # vm1.vm_start_and_get_ip(False)
    # print(vm1.iface)
    # print(vm1.get_distro())
    # vm1.set_static_ip()
    # vm1.assume_ip_is_static()

if __name__ == "__main__":
    logging.basicConfig(format='%(message)s')
    main()
