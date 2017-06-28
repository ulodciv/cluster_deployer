import json

import pytest

from pgha_deployer import HaPgCluster


@pytest.fixture
def cluster():
    with open("config/tests.json") as fh:
        cluster_json = json.load(fh)
    return HaPgCluster(cluster_def=cluster_json)


def test_ha_nodes_status(cluster, monkeypatch):

    def pcs_status_nodes(*args, **kwargs):
        return ("Pacemaker Nodes:\n"
                " Online: centos-pg-1 centos-pg-2\n"
                " Standby: centos-pg-3\n"
                " Maintenance:   \n"
                " Offline:\n"
                "Pacemaker Remote Nodes:\n"
                " Online:\n"
                " Standby:\n"
                " Maintenance:\n"
                " Offline:\n")

    cluster.master = cluster.vms[0]
    monkeypatch.setattr(cluster.master, 'ssh_run_check', pcs_status_nodes)
    d = cluster.ha_nodes_status()
    assert d == {'Online': ['centos-pg-1', 'centos-pg-2'],
                 'Standby': ['centos-pg-3'],
                 'Maintenance': [],
                 'Offline': []}


def test_ha_resource_nodes_status(cluster, monkeypatch):

    def pcs_status_xml(*args, **kwargs):
        return """\
<?xml version="1.0"?>
<crm_mon version="1.1.15">
    <summary>
        <stack type="corosync" />
        <current_dc present="true" version="1.1.15-11.el7_3.4-e174ec8" name="centos-pg-3" id="3" with_quorum="true" />
        <last_update time="Thu May 11 15:13:22 2017" />
        <last_change time="Thu May 11 15:09:04 2017" user="root" client="crm_attribute" origin="centos-pg-1" />
        <nodes_configured number="3" expected_votes="unknown" />
        <resources_configured number="4" />
        <cluster_options stonith-enabled="false" symmetric-cluster="true" no-quorum-policy="stop" />
    </summary>
    <nodes>
        <node name="centos-pg-1" id="1" online="true" standby="false" standby_onfail="false" maintenance="false" pending="false" unclean="false" shutdown="false" expected_up="true" is_dc="false" resources_running="2" type="member" />
        <node name="centos-pg-2" id="2" online="true" standby="false" standby_onfail="false" maintenance="false" pending="false" unclean="false" shutdown="false" expected_up="true" is_dc="false" resources_running="1" type="member" />
        <node name="centos-pg-3" id="3" online="true" standby="false" standby_onfail="false" maintenance="false" pending="false" unclean="false" shutdown="false" expected_up="true" is_dc="true" resources_running="1" type="member" />
    </nodes>
    <resources>
        <clone id="pgsql-ha" multi_state="true" unique="false" managed="true" failed="false" failure_ignored="false" >
            <resource id="pgsqld" resource_agent="ocf::heartbeat:pgha" role="Slave" active="true" orphaned="false" managed="true" failed="false" failure_ignored="false" nodes_running_on="1" >
                <node name="centos-pg-2" id="2" cached="false"/>
            </resource>
            <resource id="pgsqld" resource_agent="ocf::heartbeat:pgha" role="Master" active="true" orphaned="false" managed="true" failed="false" failure_ignored="false" nodes_running_on="1" >
                <node name="centos-pg-1" id="1" cached="false"/>
            </resource>
            <resource id="pgsqld" resource_agent="ocf::heartbeat:pgha" role="Slave" active="true" orphaned="false" managed="true" failed="false" failure_ignored="false" nodes_running_on="1" >
                <node name="centos-pg-3" id="3" cached="false"/>
            </resource>
        </clone>
        <resource id="pgsql-master-ip" resource_agent="ocf::heartbeat:IPaddr2" role="Started" active="true" orphaned="false" managed="true" failed="false" failure_ignored="false" nodes_running_on="1" >
            <node name="centos-pg-1" id="1" cached="false"/>
        </resource>
    </resources>
    <node_attributes>
        <node name="centos-pg-1">
            <attribute name="master-pgsqld" value="1001" />
        </node>
        <node name="centos-pg-2">
            <attribute name="master-pgsqld" value="999" />
        </node>
        <node name="centos-pg-3">
            <attribute name="master-pgsqld" value="998" />
        </node>
    </node_attributes>
    <node_history>
        <node name="centos-pg-2">
            <resource_history id="pgsqld" orphan="false" migration-threshold="5">
                <operation_history call="11" task="start" last-rc-change="Thu May 11 14:54:50 2017" last-run="Thu May 11 14:54:50 2017" exec-time="1106ms" queue-time="0ms" rc="0" rc_text="ok" />
                <operation_history call="15" task="monitor" interval="16000ms" last-rc-change="Thu May 11 14:54:52 2017" exec-time="59ms" queue-time="0ms" rc="0" rc_text="ok" />
            </resource_history>
        </node>
        <node name="centos-pg-1">
            <resource_history id="pgsql-master-ip" orphan="false" migration-threshold="5">
                <operation_history call="16" task="start" last-rc-change="Thu May 11 14:54:52 2017" last-run="Thu May 11 14:54:52 2017" exec-time="51ms" queue-time="0ms" rc="0" rc_text="ok" />
                <operation_history call="18" task="monitor" interval="10000ms" last-rc-change="Thu May 11 14:54:52 2017" exec-time="30ms" queue-time="0ms" rc="0" rc_text="ok" />
            </resource_history>
            <resource_history id="pgsqld" orphan="false" migration-threshold="5">
                <operation_history call="14" task="promote" last-rc-change="Thu May 11 14:54:51 2017" last-run="Thu May 11 14:54:51 2017" exec-time="1175ms" queue-time="1ms" rc="0" rc_text="ok" />
                <operation_history call="17" task="monitor" interval="15000ms" last-rc-change="Thu May 11 14:54:53 2017" exec-time="208ms" queue-time="1ms" rc="8" rc_text="master" />
            </resource_history>
        </node>
        <node name="centos-pg-3">
            <resource_history id="pgsqld" orphan="false" migration-threshold="5">
                <operation_history call="31" task="start" last-rc-change="Thu May 11 15:08:59 2017" last-run="Thu May 11 15:08:59 2017" exec-time="1100ms" queue-time="0ms" rc="0" rc_text="ok" />
                <operation_history call="33" task="monitor" interval="16000ms" last-rc-change="Thu May 11 15:09:00 2017" exec-time="49ms" queue-time="0ms" rc="0" rc_text="ok" />
            </resource_history>
        </node>
    </node_history>
    <tickets>
    </tickets>
    <bans>
    </bans>
</crm_mon>"""

    cluster.master = cluster.vms[0]
    monkeypatch.setattr(cluster.master, 'ssh_run_check', pcs_status_xml)
    assert cluster.ha_resource_slaves("pgsql-ha") == ["centos-pg-2", "centos-pg-3"]
    assert cluster.ha_resource_masters("pgsql-ha") == ["centos-pg-1"]
