import os
import subprocess
from unittest import TestCase

from pgsqlms2 import (
    RE_TPL_TIMELINE, RE_APP_NAME, RE_PG_CLUSTER_STATE,
    get_pg_cluster_state,
    pg_execute, get_pg_ctl_status, get_ocf_status,
    OCF_NOT_RUNNING, OCF_RUNNING_MASTER, get_ocf_nodename, get_ha_nodes,
    run_pgisready, confirm_stopped, OCF_ERR_GENERIC, OCF_SUCCESS,
    get_recovery_tpl, pg_validate_all, pg_start)


tpl = """\
primary_conninfo = 'user=postgres host=127.0.0.1 port=15432 application_name=pc1234.home'
recovery_target_timeline = 'latest'
"""
tpl2 = """\
standby_mode = 'on'
recovery_target_timeline = 'latest'
primary_conninfo = 'host=None port=15432 user=repl1 application_name=centos-pg-1'
"""
pg_conf_additions = """\
cat <<-EOC>> {}/postgresql.conf
listen_addresses = '*'
port = {}
wal_level = hot_standby
max_wal_senders = 5
hot_standby = on
hot_standby_feedback = on
wal_receiver_status_interval = 20s
EOC
"""
pg_cluster_state_output = """\
Catalog version number:               201608131
Database system identifier:           6403755386726595987
Database cluster state:               in production
pg_control last modified:             Fri 31 Mar 2017 10:01:29 PM CEST
"""


class TestPg(TestCase):
    pg_data_dir = "/tmp/pgdata_test_pgsqlms2"
    pg_bin = "/usr/pgsql-9.6/bin"
    pg_port = "15432"
    sudo = "sudo -iu postgres "

    @classmethod
    def check_call(cls, c):
        subprocess.check_call(cls.sudo + c + " &> /dev/null", shell=True)

    @classmethod
    def call(cls, c):
        subprocess.call(cls.sudo + c + " &> /dev/null", shell=True)

    @classmethod
    def start_pg(cls):
        cls.check_call("{}/pg_ctl start -D {} -w".format(
            cls.pg_bin, cls.pg_data_dir))

    @classmethod
    def stop_pg(cls):
        cls.check_call("{}/pg_ctl stop -D {} -w".format(
            cls.pg_bin, cls.pg_data_dir))

    @classmethod
    def crash_stop_pg(cls):
        cls.check_call("{}/pg_ctl stop -D {} -w -m immediate".format(
            cls.pg_bin, cls.pg_data_dir))

    @classmethod
    def setUpClass(cls):
        cls.tearDownClass()
        cls.check_call("mkdir -p {}".format(cls.pg_data_dir))
        cls.check_call("{}/initdb -D {}".format(cls.pg_bin, cls.pg_data_dir))
        cls.check_call(pg_conf_additions.format(cls.pg_data_dir, cls.pg_port))
        os.environ['OCF_RESKEY_pgdata'] = cls.pg_data_dir
        os.environ['OCF_RESKEY_bindir'] = cls.pg_bin
        os.environ['OCF_RESKEY_pgport'] = cls.pg_port
        os.environ['OCF_RESOURCE_INSTANCE'] = "foo"
        cls.start_pg()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.crash_stop_pg()
        except subprocess.CalledProcessError:
            pass
        cls.call("rm -rf {}".format(cls.pg_data_dir))

    def setUp(self):
        try:
            self.stop_pg()
        except subprocess.CalledProcessError:
            pass

    def test_pg_validate_all(self):
        with open(get_recovery_tpl(), "w") as f:
            f.write(tpl2)
        rc = pg_validate_all()
        os.remove(get_recovery_tpl())
        self.assertEqual(OCF_SUCCESS, rc)

    def test_pgsql_start(self):
        self.assertRaises(SystemExit, pg_start)
        with open(get_recovery_tpl(), "w") as f:
            f.write(tpl2)
        self.assertEqual(OCF_SUCCESS, pg_start())

    def test_run_pgisready(self):
        rc = run_pgisready()
        self.assertEqual(2, rc)
        self.start_pg()
        rc = run_pgisready()
        self.assertEqual(0, rc)

    def test_confirm_stopped(self):
        self.assertEqual(OCF_NOT_RUNNING, confirm_stopped())
        self.start_pg()
        self.assertEqual(OCF_ERR_GENERIC, confirm_stopped())

    def test_get_pg_ctl_status(self):
        self.assertEqual(3, get_pg_ctl_status())
        self.start_pg()
        self.assertEqual(0, get_pg_ctl_status())
        self.stop_pg()
        self.assertEqual(3, get_pg_ctl_status())

    def test_get_pg_cluster_state(self):
        state = get_pg_cluster_state()
        self.assertEqual('shut down', state)
        self.start_pg()
        state = get_pg_cluster_state()
        self.assertEqual('in production', state)
        self.stop_pg()
        state = get_pg_cluster_state()
        self.assertEqual('shut down', state)

    def test_pg_execute(self):
        self.start_pg()
        rc, rs = pg_execute("SELECT pg_last_xlog_receive_location()")
        self.assertEqual(0, rc)
        rc, rs = pg_execute("SELECT 25")
        self.assertEqual(0, rc)
        self.assertEqual([["25"]], rs)

    def test_get_ocf_status_from_pg_cluster_state(self):
        ocf_status = get_ocf_status()
        self.assertEqual(ocf_status, OCF_NOT_RUNNING)
        self.start_pg()
        ocf_status = get_ocf_status()
        self.assertEqual(ocf_status, OCF_RUNNING_MASTER)

    def test_dummy(self):
        self.assertTrue(True)


class TestHa(TestCase):

    def test_get_ha_nodes(self):
        nodes = get_ha_nodes()
        self.assertIn('centos-pg-1', nodes)

    def test_get_ocf_nodename(self):
        n = get_ocf_nodename()
        self.assertEqual(n, 'centos-pg-1')


class TestRegExes(TestCase):
    def test_tpl_file(self):
        m = RE_TPL_TIMELINE.search(tpl)
        self.assertIsNotNone(m)

    def test_app_name(self):
        m = RE_APP_NAME.findall(tpl)
        self.assertIsNotNone(m)
        self.assertEqual(m[0], "pc1234.home")

    def test_pg_cluster_state(self):
        m = RE_PG_CLUSTER_STATE.findall(pg_cluster_state_output)
        self.assertIsNotNone(m)
        self.assertEqual(m[0], "in production")
