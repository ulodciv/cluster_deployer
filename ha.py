from abc import ABCMeta
from ipaddress import ip_interface, IPv4Interface, IPv4Address

from ssh import Ssh


class HA(Ssh, metaclass=ABCMeta):
    ha_cluster_xml_file = f"cluster.xml"

    def __init__(self, *, cluster, cluster_vip, **kwargs):
        super(HA, self).__init__(**kwargs)
        self.cluster = cluster
        self.cluster_vip = cluster_vip

    def ha_base_setup(self, vms):
        """
        pcs cluster auth pg01 pg02
        pcs cluster setup --start --name pgcluster pg01 pg02
        pcs cluster start --all
        """
        hosts = " ".join(vm.name for vm in vms)
        self.ssh_run_check(
            [f"pcs cluster auth {hosts} -u hacluster -p hacluster",
             f"pcs cluster setup --start --name {self.cluster} {hosts}",
             "pcs cluster start --all"])

    def ha_get_vip_ipv4(self):
        if type(self.cluster_vip) is IPv4Interface:
            return self.cluster_vip
        if type(self.cluster_vip) is IPv4Address:
            return IPv4Interface(str(self.cluster_vip) + "/24")
        if "/" in self.cluster_vip:
            return ip_interface(self.cluster_vip)
        return IPv4Interface(self.cluster_vip + "/24")

    def ha_create_vip(self):
        ipv4 = self.ha_get_vip_ipv4()
        self.ssh_run_check(
            f"pcs resource create ClusterVIP ocf:heartbeat:IPaddr2 "
            f"ip={ipv4.ip} cidr_netmask={ipv4.network.prefixlen}")

    def ha_drop_vip(self):
        self.ssh_run_check(f"pcs resource delete ClusterVIP")

    def ha_disable_stonith(self):
        self.ssh_run_check("pcs property set stonith-enabled=false")

    def ha_disable_quorum(self):
        self.ssh_run_check("pcs property set no-quorum-policy=ignore")

    def ha_set_resource_stickiness(self, v: int):
        self.ssh_run_check(f"pcs resource defaults resource-stickiness={v}")

    def ha_set_migration_threshold(self, v: int):
        self.ssh_run_check(f"pcs resource defaults migration-threshold={v}")

    def ha_export_xml(self):
        self.ssh_run_check(f"pcs cluster cib {HA.ha_cluster_xml_file}")

    def ha_add_pg_to_xml(self):
        self.ssh_run_check(
            f"pcs -f {HA.ha_cluster_xml_file} "
            f"resource create pgsqld ocf:heartbeat:pgsqlms "
            f"bindir=/usr/pgsql-9.6/bin pgdata=/var/lib/pgsql/9.6/data "
            f"op start timeout=60s "
            f"op stop timeout=60s "
            f"op promote timeout=30s "
            f"op demote timeout=120s "
            f"op monitor interval=15s timeout=10s role=\"Master\" "
            f"op monitor interval=16s timeout=10s role=\"Slave\" "
            f"op notify timeout=60s")
        self.ssh_run_check(
            f"pcs -f {HA.ha_cluster_xml_file} "
            f"resource master pgsql-ha pgsqld "
            f"master-max=1 master-node-max=1 "
            f"clone-max=3 clone-node-max=1 notify=true")

    def ha_add_pg_vip_to_xml(self):
        ipv4 = self.ha_get_vip_ipv4()
        self.ssh_run_check(
            f"pcs -f {HA.ha_cluster_xml_file} "
            f"resource create pgsql-master-ip ocf:heartbeat:IPaddr2 "
            f"ip={ipv4.ip} cidr_netmask={ipv4.network.prefixlen}")
        self.ssh_run_check(
            f"pcs -f {HA.ha_cluster_xml_file} "
            f"constraint colocation add pgsql-master-ip "
            f"with master pgsql-ha INFINITY")
        self.ssh_run_check(
            f"pcs -f {HA.ha_cluster_xml_file} "
            f"constraint order promote pgsql-ha "
            f"then start pgsql-master-ip symmetrical=false")
        self.ssh_run_check(
            f"pcs -f {HA.ha_cluster_xml_file} "
            f"constraint order demote pgsql-ha "
            f"then stop pgsql-master-ip symmetrical=false")

    def ha_import_xml(self):
        self.ssh_run_check(f"pcs cluster cib-push {HA.ha_cluster_xml_file}")
