from concurrent.futures import ThreadPoolExecutor, as_completed

from ha import HA
from postgres import Postgres
from vm import Vbox


def raise_first(futures):
    for future in as_completed(futures):
        if future.exception():
            raise future.exception()


class Cluster:
    def __init__(self, hosts):
        self.vms = [Vm(**h) for h in hosts]

    def deploy(self):
        self.deploy_part_1()
        self.deploy_part_2()
        self.deploy_part_3()
        self.deploy_part_4()
        self.deploy_part_5()

    @property
    def master(self):
        return self.vms[0]

    @property
    def standbies(self):
        return self.vms[1:]

    def deploy_part_1(self):
        with ThreadPoolExecutor() as e:
            raise_first([e.submit(v.deploy, self.vms) for v in self.vms])

    def deploy_part_2(self):
        with ThreadPoolExecutor() as e:
            raise_first([e.submit(
                v.put_ssh_key_on_others, self.vms) for v in self.vms])

    def deploy_part_3(self):
        master = self.master
        master.pg_start()
        master.deploy_demo_db()
        master.pg_create_replication_user()
        master.pg_make_master(self.vms)
        master.pg_restart()
        # master.pg_add_replication_slots(vms)
        master.pg_write_recovery_for_pcmk()
        master.add_temp_ipv4_to_iface(master.ha_get_vip_ipv4())

    def deploy_part_4(self):
        with ThreadPoolExecutor() as e:
            raise_first(
                [e.submit(s.pg_standby_backup_from_master, self.master)
                 for s in self.standbies])
            raise_first([e.submit(s.pg_start) for s in self.standbies])
            raise_first([e.submit(vm.pg_stop) for vm in self.vms])

    def deploy_part_5(self):
        self.master.del_temp_ipv4_to_iface(self.master.ha_get_vip_ipv4())
        self.master.deploy_ha(self.vms)


class Vm(Vbox, Postgres, HA):
    def __init__(self, **kwargs):
        super(Vm, self).__init__(**kwargs)

    def deploy(self, vms):
        self.vm_deploy(False)
        self.vm_start_and_get_ip(False)
        self.setup_users()
        self.set_hostname()
        self.set_static_ip()
        self.add_hosts_to_etc_hosts(vms)
        self.pg_create_wal_dir()

    def deploy_demo_db(self):
        db = "demo_db"
        self.ssh_run_check(
            [f"tar -xf {db}.xz",
             f"mv {db} /tmp/",
             f"chown -R {self.pg_root_user}. /tmp/{db}"])
        self.pg_restore_db(db, f"/tmp/{db}/install.sql", f"/tmp/{db}")
        self.ssh_run_check(f'rm -rf /tmp/{db}')

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