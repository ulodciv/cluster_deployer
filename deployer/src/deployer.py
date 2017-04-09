from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from pathlib import Path, PurePosixPath
from ipaddress import ip_interface, IPv4Interface, IPv4Address

from paramiko import RSAKey

from postgres import Postgres
from vm import Vbox


def raise_first(futures):
    for future in as_completed(futures):
        if future.exception():
            raise future.exception()


class Cluster:

    def __init__(self, cluster_def, no_threads=False):
        self.ha_cluster_xml_file = f"cluster.xml"
        self.cluster_name = cluster_def["cluster_name"]
        self.virtual_ip = cluster_def["virtual_ip"]
        self.demo_db = cluster_def["demo_db"]
        self.pg_ra = cluster_def["pg_ra"]
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
        self.vms = [Vm(**h) for h in hosts]
        self.no_threads = no_threads

    def deploy(self):
        # parts 1 and 2 can safely be re-run
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

    def call(self, calls):
        if self.no_threads:
            [c() for c in calls]
        else:
            with ThreadPoolExecutor() as e:
                raise_first([e.submit(c) for c in calls])

    def deploy_part_1(self):
        self.call([partial(v.deploy, self.vms) for v in self.vms])

    def deploy_part_2(self):
        self.call(
            [partial(v.authorize_keys, self.vms) for v in self.vms])
        self.call(
            [partial(v.add_fingerprints, self.vms) for v in self.vms])

    def deploy_part_3(self):
        remote_ra = "/usr/lib/ocf/resource.d/heartbeat/pgsqlms2"
        for vm in self.vms:
            vm.sftp_put(self.pg_ra, remote_ra)
            vm.ssh_run_check(f"chmod +x {remote_ra}")
        master = self.master
        master.pg_start()
        master.deploy_demo_db(self.demo_db)
        master.pg_create_replication_user()
        master.pg_make_master(self.vms)
        master.pg_restart()
        master.pg_add_replication_slots(self.vms)
        master.pg_write_recovery_for_pcmk(self.virtual_ip)
        master.add_temp_ipv4_to_iface(self.ha_get_vip_ipv4())

    def deploy_part_4(self):
        self.call([partial(s.pg_standby_backup_from_master, self.master)
                   for s in self.standbies])
        self.call([partial(s.pg_start) for s in self.standbies])
        self.call([partial(vm.pg_stop) for vm in self.vms])

    def deploy_part_5(self):
        self.master.del_temp_ipv4_to_iface(self.ha_get_vip_ipv4())
        self.ha_base_setup(self.vms)
        self.ha_set_migration_threshold(5)
        self.ha_set_resource_stickiness(10)
        self.ha_disable_stonith()
        self.ha_export_xml()
        self.ha_add_pg_to_xml()
        self.ha_add_pg_vip_to_xml()
        self.ha_import_xml()
        # self.master.ha_create_vip()

    def ha_base_setup(self, vms):
        """
        pcs cluster auth pg01 pg02
        pcs cluster setup --start --name pgcluster pg01 pg02
        pcs cluster start --all
        """
        hosts = " ".join(vm.name for vm in vms)
        self.master.ssh_run_check(
            [f"pcs cluster auth {hosts} -u hacluster -p hacluster",
             f"pcs cluster setup --start --name {self.cluster_name} {hosts}",
             "pcs cluster start --all"])

    def ha_get_vip_ipv4(self):
        if type(self.virtual_ip) is IPv4Interface:
            return self.virtual_ip
        if type(self.virtual_ip) is IPv4Address:
            return IPv4Interface(str(self.virtual_ip) + "/24")
        if "/" in self.virtual_ip:
            return ip_interface(self.virtual_ip)
        return IPv4Interface(self.virtual_ip + "/24")

    def ha_create_vip(self):
        ipv4 = self.ha_get_vip_ipv4()
        self.master.ssh_run_check(
            f"pcs resource create ClusterVIP ocf:heartbeat:IPaddr2 "
            f"ip={ipv4.ip} cidr_netmask={ipv4.network.prefixlen}")

    def ha_drop_vip(self):
        self.master.ssh_run_check(f"pcs resource delete ClusterVIP")

    def ha_disable_stonith(self):
        self.master.ssh_run_check("pcs property set stonith-enabled=false")

    def ha_disable_quorum(self):
        self.master.ssh_run_check("pcs property set no-quorum-policy=ignore")

    def ha_set_resource_stickiness(self, v: int):
        self.master.ssh_run_check(
            f"pcs resource defaults resource-stickiness={v}")

    def ha_set_migration_threshold(self, v: int):
        self.master.ssh_run_check(
            f"pcs resource defaults migration-threshold={v}")

    def ha_export_xml(self):
        self.master.ssh_run_check(
            f"pcs cluster cib {self.ha_cluster_xml_file}")

    def ha_add_pg_to_xml(self):
        self.master.ssh_run_check(
            f"pcs -f {self.ha_cluster_xml_file} "
            f"resource create pgsqld ocf:heartbeat:pgsqlms2 "
            f"bindir={self.master.pg_bindir} "
            f"pgdata={self.master.pg_data_directory} "
            f"pghost=/var/run/postgresql "
            f"start_opts=\"{self.master.pg_start_opts}\" "
            f"op start timeout=60s "
            f"op stop timeout=60s "
            f"op promote timeout=30s "
            f"op demote timeout=120s "
            f"op monitor interval=15s timeout=10s role=\"Master\" "
            f"op monitor interval=16s timeout=10s role=\"Slave\" "
            f"op notify timeout=60s")
        self.master.ssh_run_check(
            f"pcs -f {self.ha_cluster_xml_file} "
            f"resource master pgsql-ha pgsqld "
            f"master-max=1 master-node-max=1 "
            f"clone-max=3 clone-node-max=1 notify=true")

    def ha_add_pg_vip_to_xml(self):
        ipv4 = self.ha_get_vip_ipv4()
        self.master.ssh_run_check(
            f"pcs -f {self.ha_cluster_xml_file} "
            f"resource create pgsql-master-ip ocf:heartbeat:IPaddr2 "
            f"ip={ipv4.ip} cidr_netmask={ipv4.network.prefixlen}")
        self.master.ssh_run_check(
            f"pcs -f {self.ha_cluster_xml_file} constraint colocation add "
            f"pgsql-master-ip with master pgsql-ha INFINITY")
        self.master.ssh_run_check(
            f"pcs -f {self.ha_cluster_xml_file} constraint order promote "
            f"pgsql-ha then start pgsql-master-ip symmetrical=false")
        self.master.ssh_run_check(
            f"pcs -f {self.ha_cluster_xml_file} constraint order demote "
            f"pgsql-ha then stop pgsql-master-ip symmetrical=false")

    def ha_import_xml(self):
        self.master.ssh_run_check(
            f"pcs cluster cib-push {self.ha_cluster_xml_file}")


class Vm(Vbox, Postgres):
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

    def deploy_demo_db(self, demo_db_file):
        local_db_file = Path(demo_db_file)
        db_file_name = local_db_file.name
        remote_db_file = PurePosixPath("/tmp") / db_file_name
        self.sftp_put(local_db_file, remote_db_file, self.pg_user)
        db = local_db_file.stem
        self.ssh_run_check(
            f"cd /tmp && tar -xf {db_file_name} && rm -f {db_file_name}",
            self.pg_user)
        self.pg_drop_db(db)
        self.pg_create_db(db)
        self.ssh_run_check(
            f"cd /tmp/{db} && psql -v ON_ERROR_STOP=1 -t -q -f install.sql {db}",
            self.pg_user)
        self.ssh_run_check(f'rm -rf /tmp/{db}')
