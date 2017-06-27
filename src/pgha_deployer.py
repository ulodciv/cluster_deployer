import logging
import json
from argparse import ArgumentParser
from datetime import timedelta
from functools import partial
from ipaddress import IPv4Interface, ip_interface, IPv4Address
from pathlib import PurePosixPath, Path
from time import time

from deployerlib.cluster import Cluster
from deployerlib.ha import HA
from deployerlib.pg import PG
from deployerlib.vm import VBox


class HaPgVm(HA, PG, VBox):
    def __init__(self, **kwargs):
        super(HaPgVm, self).__init__(**kwargs)

    def pgha_deploy_db(self, demo_db_file):
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
            f"cd /tmp/{db} && {self.psql} -p {self.pg_port} -v ON_ERROR_STOP=1 "
            f"-t -q -f install.sql {db}",
            user=self.pg_user)
        self.ssh_run_check(f'rm -rf /tmp/{db}')


class HaPgCluster(Cluster):

    def __init__(self, *, cluster_def, **kwargs):
        super(HaPgCluster, self).__init__(
            cluster_def=cluster_def, vm_class=HaPgVm, **kwargs)
        self.master = None
        self.demo_db = cluster_def["demo_db"]
        self.pgha_file = cluster_def["pgha_file"]
        self.pgha_resource = cluster_def["pgha_resource"]
        self.pgha_resource_master = f"{self.pgha_resource}-master"
        self.pgha_resource_master_ip = f"{self.pgha_resource_master}-ip"
        self.virtual_ip = cluster_def["virtual_ip"]

    def deploy(self):
        self.deploy_base()
        self.pgha_put_pgha_on_nodes()
        self.pgha_setup_master()
        self.pgha_setup_slaves()
        self.pgha_setup_ra()

    @property
    def standbies(self):
        return [vm for vm in self.vms if vm != self.master]

    def pgha_put_pgha_on_nodes(self):
        remote_ra = "/usr/lib/ocf/resource.d/heartbeat/pgha"
        for vm in self.vms:
            vm.sftp_put(self.pgha_file, remote_ra)
            vm.ssh_run_check(f"chmod +x {remote_ra}")

    def pgha_setup_master(self):
        self.master = self.vms[0]
        master = self.master
        master.pg_start()
        master.pgha_deploy_db(self.demo_db)
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
        master.pg_add_replication_slots(self.standbies)

    def pgha_setup_slaves(self):
        master = self.master
        self.call([partial(m.pg_backup, master) for m in self.standbies])
        for vm in self.vms:
            if vm == master:
                vm.pg_write_recovery_conf()
            else:
                vm.pg_write_recovery_conf(master.name)
        self.call([partial(m.pg_start) for m in self.standbies])
        self.call([partial(m.pg_stop) for m in self.vms])

    def pgha_setup_ra(self):
        master = self.master
        master.ha_base_setup(self.vms)
        master.ha_set_migration_threshold(5)
        master.ha_set_resource_stickiness(10)
        master.ha_disable_stonith()
        self.pgha_configure_cib()

    def ha_get_vip_ipv4(self):
        if type(self.virtual_ip) is IPv4Interface:
            return self.virtual_ip
        if type(self.virtual_ip) is IPv4Address:
            return IPv4Interface(str(self.virtual_ip) + "/24")
        if "/" in self.virtual_ip:
            return ip_interface(self.virtual_ip)
        return IPv4Interface(self.virtual_ip + "/24")

    def pgha_configure_cib(self):
        master = self.master
        master.ha_get_cib()
        # pg_host: tcp or unix_socket_directories?
        if "unix_socket_directories" in master.pg_conf_dict:
            l = master.pg_conf_dict["unix_socket_directories"].split(",")
            if len(l) > 0 and l[0]:
                pg_host = l[0]
            else:
                pg_host = "localhost"
        else:
            pg_host = "/tmp"
        # pgha
        master.ha_pcs_xml(
            f'resource create {self.pgha_resource} ocf:heartbeat:pgha '
            f'pgbindir={master.pg_bindir} '
            f'pgdata={master.pg_data_directory} '
            f'pgconf={master.pg_config_file} '
            f'pgport={master.pg_port} '
            f'pghost={pg_host} '
            f'op start timeout=60s '
            f'op stop timeout=60s '
            f'op promote timeout=120s '
            f'op demote timeout=120s '
            f'op monitor interval=5s timeout=10s role="Master" '
            f'op monitor interval=6s timeout=10s role="Slave" '
            f'op notify timeout=60s')
        master.ha_pcs_xml(
            f"resource master {self.pgha_resource_master} {self.pgha_resource} "
            f"clone-max=10 notify=true")
        # VIP
        ipv4 = self.ha_get_vip_ipv4()
        pgha_resource_master_ip = self.pgha_resource_master_ip
        master.ha_pcs_xml(
            f"resource create {pgha_resource_master_ip} ocf:heartbeat:IPaddr2 "
            f"ip={ipv4.ip} cidr_netmask={ipv4.network.prefixlen}")
        master.ha_pcs_xml(
            f"constraint colocation add {pgha_resource_master_ip} "
            f"with master {self.pgha_resource_master} INFINITY")
        master.ha_pcs_xml(
            f"constraint order promote {self.pgha_resource_master} "
            f"then start {pgha_resource_master_ip}")
        master.ha_cib_push()


def parse_args():
    parser = ArgumentParser(description='Deploy a cluster')
    parser.add_argument("json_file", help="Cluster definition (JSON)")
    parser.add_argument('--use-threads', action='store_true', default=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    start = time()
    with open(args.json_file) as f:
        cluster = HaPgCluster(
            cluster_def=json.load(f), use_threads=args.use_threads)
    cluster.deploy()
    logging.getLogger("main").debug(f"took {timedelta(seconds=time() - start)}")
