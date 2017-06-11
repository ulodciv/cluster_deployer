import xml.etree.ElementTree as ET

from deploylib.ssh import Ssh


class Ha(Ssh):
    ha_cluster_xml_file = f"cluster.xml"

    def __init__(self, *, cluster_name, **kwargs):
        super(Ha, self).__init__(**kwargs)
        self.cluster_name = cluster_name

    def ha_base_setup(self, vms):
        """
        pcs cluster auth pg01 pg02
        pcs cluster setup --start --name pgcluster pg01 pg02
        pcs cluster start --all
        """
        hosts = " ".join(vm.name for vm in vms)
        self.ssh_run_check(
            [f"pcs cluster auth {hosts} -u hacluster -p hacluster",
             f"pcs cluster setup --start --name {self.cluster_name} {hosts}",
             "pcs cluster start --all"])

    def _pcs_cluster(self, cmd):
        self.ssh_run_check(f"pcs cluster {cmd}")

    def ha_standby(self, vm):
        self._pcs_cluster(f"standby {vm.name}")

    def ha_unstandby(self, vm):
        self._pcs_cluster(f"unstandby {vm.name}")

    def ha_standby_all(self):
        self._pcs_cluster("standby --all")

    def ha_unstandby_all(self):
        self._pcs_cluster("unstandby --all")

    def ha_stop_all(self):
        self._pcs_cluster("stop --all")

    def ha_start_all(self):
        self._pcs_cluster("start --all")

    def ha_stop(self, vm):
        self._pcs_cluster(f"stop {vm.name}")

    def ha_start(self, vm):
        self._pcs_cluster(f"start {vm.name}")

    def ha_get_cib(self):
        self._pcs_cluster(f"cib {self.ha_cluster_xml_file}")

    def ha_cib_push(self):
        self._pcs_cluster(f"cib-push {self.ha_cluster_xml_file}")

    def ha_nodes_status(self):
        o = self.ssh_run_check("pcs status nodes", get_output=True)
        d = {}
        for l in o.splitlines():
            stripped = l.strip()
            if stripped.startswith("Pacemaker Nodes:"):
                continue
            if stripped.startswith("Pacemaker Remote Nodes:"):
                break
            pieces = l.strip().split()
            d[pieces[0].strip(":")] = pieces[1:]
        return d

    def ha_resource_slaves_masters(self, clone_id):
        """
        crm_mon -> resources -> clone -> [resource1, resource2, ...]
        """
        slaves = []
        masters = []
        o = self.ssh_run_check("pcs status xml", get_output=True)
        root = ET.fromstring(o)
        for child1 in root:
            if child1.tag != "resources":
                continue
            for child2 in child1:
                if child2.tag != "clone" or child2.attrib["id"] != clone_id:
                    continue
                for child3 in child2:
                    if child3.tag != "resource":
                        continue
                    if child3.attrib["failed"] == "true":
                        continue
                    if child3.attrib["active"] != "true":
                        continue
                    if child3.attrib["role"] == "Slave":
                        for child4 in child3:
                            slaves.append(child4.attrib["name"])
                    if child3.attrib["role"] == "Master":
                        for child4 in child3:
                            masters.append(child4.attrib["name"])
        return slaves, masters

    def ha_resource_slaves(self, clone_id):
        return self.ha_resource_slaves_masters(clone_id)[0]

    def ha_resource_masters(self, clone_id):
        return self.ha_resource_slaves_masters(clone_id)[1]

    def ha_pcs_xml(self, what):
        self.ssh_run_check(f"pcs -f {self.ha_cluster_xml_file} {what}")

    def ha_drop_vip(self):
        self.ssh_run_check("pcs resource delete ClusterVIP")

    def ha_disable_stonith(self):
        self.ssh_run_check("pcs property set stonith-enabled=false")

    def ha_disable_quorum(self):
        self.ssh_run_check("pcs property set no-quorum-policy=ignore")

    def ha_set_resource_stickiness(self, v: int):
        self.ssh_run_check(f"pcs resource defaults resource-stickiness={v}")

    def ha_set_migration_threshold(self, v: int):
        self.ssh_run_check(f"pcs resource defaults migration-threshold={v}")

    def ha_get_scores_dict(self, resource):
        s = self.ssh_run_check("crm_node -p", get_output=True)
        d = {n: self.ha_get_score(resource, n) for n in s.split()}
        return {k: int(v) for k, v in d.items() if v}

    def ha_get_score(self, resource, node=None):
        cmd = f"crm_master -r {resource}"
        if node:
            cmd += f" -N {node}"
        s = self.ssh_run_check(cmd, get_output=True)
        if not s:
            return None
        # scope=nodes  name=master-pg value=998
        parts = [p.partition("=") for p in s.split()]
        d = {p[0]: p[2] for p in parts}
        return d.get("value", None)

