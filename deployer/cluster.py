import logging
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from pathlib import Path, PurePosixPath
from ipaddress import ip_interface, IPv4Interface, IPv4Address

from paramiko import RSAKey

from ha import Ha
from postgres import Postgres
from vm import Vbox


def raise_first(futures):
    for future in as_completed(futures):
        if future.exception():
            raise future.exception()


class ClusterBase:

    @staticmethod
    def configure_logging():
        logging.basicConfig(format='%(asctime)s %(name)s: %(message)s')
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("paramiko").setLevel(logging.WARNING)

    def __init__(self, *, cluster_def, vm_class, use_threads, **kwargs):
        super(ClusterBase, self).__init__(**kwargs)
        self.use_threads = use_threads
        self.configure_logging()
        common = {k: v for k, v in cluster_def.items() if k != "hosts"}
        common["paramiko_key"] = RSAKey.from_private_key_file(
            common["key_file"])
        with open(common["pub_key_file"]) as f:
            common["paramiko_pub_key"] = f.read()
        hosts = [
            {
                **common,
                **host
            } for host in cluster_def["hosts"]
        ]
        self.vms = [vm_class(**h) for h in hosts]

    def call(self, calls):
        if not self.use_threads:
            [c() for c in calls]
        else:
            with ThreadPoolExecutor() as e:
                raise_first([e.submit(c) for c in calls])

    @abstractmethod
    def deploy(self):
        pass


class VboxPgHaCluster(ClusterBase):

    def __init__(self, *, cluster_def, **kwargs):
        super(VboxPgHaCluster, self).__init__(
            cluster_def=cluster_def, vm_class=VboxPgVm, **kwargs)
        self.master = None
        self.demo_db = cluster_def["demo_db"]
        self.pgha_file = cluster_def["pgha_file"]
        self.pgha_resource = cluster_def["pgha_resource"]
        self.virtual_ip = cluster_def["virtual_ip"]

    @property
    def pgha_resource_master(self):
        return f"{self.pgha_resource}-master"

    def deploy(self):
        # parts 1 and 2 can safely be re-run
        self.deploy_part_1()
        self.deploy_part_2()
        self.deploy_part_3()
        self.deploy_part_4()
        self.deploy_part_5()

    @property
    def standbies(self):
        return [vm for vm in self.vms if vm != self.master]

    def deploy_part_1(self):
        self.call([partial(v.deploy, self.vms) for v in self.vms])

    def deploy_part_2(self):
        self.call([partial(v.authorize_keys, self.vms) for v in self.vms])
        self.call([partial(v.add_fingerprints, self.vms) for v in self.vms])

    def deploy_part_3(self):
        remote_ra = "/usr/lib/ocf/resource.d/heartbeat/pgha"
        for vm in self.vms:
            vm.sftp_put(self.pgha_file, remote_ra)
            vm.ssh_run_check(f"chmod +x {remote_ra}")
        self.master = self.vms[0]
        master = self.master
        master.pg_start()
        master.deploy_demo_db(self.demo_db)
        master.pg_create_replication_user()
        hba_file = master.pg_hba_file
        for vm in self.vms:
            cmds = [
                f'echo "host replication {h.pg_repl_user} {h.ip}/32 trust" ' 
                f'>> {hba_file}'
                for h in self.vms]
            vm.ssh_run_check(cmds, user=vm.pg_user)
        master.pg_make_master(self.vms)
        master.pg_restart()
        _ = master.pg_config_file
        master.pg_add_replication_slots(self.vms)
        master.pg_write_recovery_for_pcmk(self.virtual_ip)
        master.add_temp_ipv4_to_iface(self.ha_get_vip_ipv4())

    def deploy_part_4(self):
        self.call([partial(s.pg_standby_backup_from_master, self.master)
                   for s in self.standbies])
        self.call([partial(s.pg_start) for s in self.standbies])
        self.call([partial(vm.pg_stop) for vm in self.vms])

    def deploy_part_5(self):
        master = self.master
        master.del_temp_ipv4_to_iface(self.ha_get_vip_ipv4())
        master.ha_base_setup(self.vms)
        master.ha_set_migration_threshold(5)
        master.ha_set_resource_stickiness(10)
        master.ha_disable_stonith()
        master.ha_get_cib()
        self.ha_add_pg_to_xml()
        self.ha_add_pg_vip_to_xml()
        master.ha_cib_push()

    def ha_get_vip_ipv4(self):
        if type(self.virtual_ip) is IPv4Interface:
            return self.virtual_ip
        if type(self.virtual_ip) is IPv4Address:
            return IPv4Interface(str(self.virtual_ip) + "/24")
        if "/" in self.virtual_ip:
            return ip_interface(self.virtual_ip)
        return IPv4Interface(self.virtual_ip + "/24")

    def ha_add_pg_to_xml(self):
        master = self.master
        master._pcs_xml(
            f"resource create {self.pgha_resource} ocf:heartbeat:pgha "
            f"pgbindir={master.pg_bindir} "
            f"pgdata={master.pg_datadir} "
            f"pgconf={master.pg_config_file} "
            f"pgport={master.pg_port} "
            f"op start timeout=60s "
            f"op stop timeout=60s "
            f"op promote timeout=120s "
            f"op demote timeout=120s "
            f"op monitor interval=10s timeout=10s role=\"Master\" "
            f"op monitor interval=11s timeout=10s role=\"Slave\" "
            f"op notify timeout=60s")
        master._pcs_xml(
            f"resource master {self.pgha_resource_master} {self.pgha_resource} "
            f"clone-max=10 notify=true")

    def ha_add_pg_vip_to_xml(self):
        ipv4 = self.ha_get_vip_ipv4()
        master = self.master
        master._pcs_xml(
            f"resource create pgha-master-vip ocf:heartbeat:IPaddr2 "
            f"ip={ipv4.ip} cidr_netmask={ipv4.network.prefixlen}")
        master._pcs_xml(
            f"constraint colocation add "
            f"pgha-master-vip with master {self.pgha_resource_master} INFINITY")
        master._pcs_xml(
            f"constraint order promote {self.pgha_resource_master} "
            f"then start pgha-master-vip symmetrical=false")
        master._pcs_xml(
            f"constraint order demote {self.pgha_resource_master} "
            f"then stop pgha-master-vip symmetrical=false")


class VboxPgVm(Vbox, Ha, Postgres):
    def __init__(self, **kwargs):
        super(VboxPgVm, self).__init__(**kwargs)

    def deploy(self, vms):
        self.vm_deploy(False)
        self.vm_start_and_get_ip(False)
        self.wait_until_port_is_open(22, 10)
        self.setup_users()
        self.set_hostname()
        self.set_static_ip()
        self.add_hosts_to_etc_hosts(vms)

    def deploy_demo_db(self, demo_db_file):
        local_db_file = Path(demo_db_file)
        db_file_name = local_db_file.name
        remote_db_file = PurePosixPath("/tmp") / db_file_name
        self.sftp_put(local_db_file, remote_db_file, self.pg_user)
        db = local_db_file.stem
        self.ssh_run_check(
            f"cd /tmp && tar -xf {db_file_name} && rm -f {db_file_name}",
            user=self.pg_user)
        self.pg_drop_db(db)
        self.pg_create_db(db)
        self.ssh_run_check(
            f"cd /tmp/{db} && psql -p {self.pg_port} -v ON_ERROR_STOP=1 -t -q "
            f"-f install.sql {db}",
            user=self.pg_user)
        self.ssh_run_check(f'rm -rf /tmp/{db}')
