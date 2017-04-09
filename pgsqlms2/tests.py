#!/usr/bin/python2.7
import os
import re
import subprocess
import pwd
import logging
from subprocess import check_call, call
from unittest import TestCase


from pgsqlms2 import (
    RE_TPL_TIMELINE, RE_APP_NAME, RE_PG_CLUSTER_STATE,
    get_pg_cluster_state,
    pg_execute, get_pg_ctl_status, get_ocf_status,
    OCF_NOT_RUNNING, get_ocf_nodename, get_ha_nodes,
    run_pgisready, confirm_stopped, OCF_ERR_GENERIC, OCF_SUCCESS,
    get_recovery_tpl, pg_validate_all, pg_start, get_connected_standbies)


# returns CENTOS, UBUNTU, DEBIAN, or something else
def get_distro():
    with open("/etc/os-release") as f:
        s = f.read()
    p = re.compile(r'^ID="?(?P<distro>\w+)"?', re.M)
    return p.findall(s)[0].upper()


def change_user_to_postgres():
    uid, gid = pwd.getpwnam("postgres")[2:4]
    os.initgroups("postgres", gid)
    os.setgid(gid)
    os.setuid(uid)
    os.seteuid(uid)


def write_as_pg(f, s):
    uid, gid = pwd.getpwnam("postgres")[2:4]
    with open(f, "w") as fh:
        fh.write(s)
    os.chown(f, uid, gid)


def run_as_pg(cmd, check=True):
    logging.info("running " + cmd + '\n')
    with open(os.devnull, 'w') as DEVNULL:
        if check:
            return check_call(
                cmd, shell=True,
                preexec_fn=change_user_to_postgres)
                # stdout=DEVNULL, stderr=STDOUT)
        else:
            return call(
                cmd, shell=True,
                preexec_fn=change_user_to_postgres)
                # stdout=DEVNULL, stderr=STDOUT)


class TestPg(TestCase):
    pguser = "postgres"
    pguid, pggid = pwd.getpwnam("postgres")[2:4]
    uname = subprocess.check_output("uname -n", shell=True).strip()
    pgversion = "9.6"
    pgdata_master = "/tmp/pgsqlms2_test_master"
    pgdata_slave = "/tmp/pgsqlms2_test_slave"
    pgport_master = "15432"
    pgport_slave = "15433"
    linux_distro = get_distro()
    if linux_distro == "CENTOS":
        pgbin = "/usr/pgsql-{}/bin".format(pgversion)
    elif linux_distro in ("DEBIAN", "UBUNTU"):
        pgbin = "/usr/lib/postgresql/{}/bin".format(pgversion)
    else:
        raise Exception("unknown linux distro: " + linux_distro)
    pgstart_opts = "'-c config_file={}/postgresql.conf'"
    tpl = """\
standby_mode = on
recovery_target_timeline = latest
primary_conninfo = 'host=/var/run/postgresql port=15432 user=postgres application_name={}'
""".format(uname)
    conf_additions = """
listen_addresses = '*'
port = {}
wal_level = replica
max_wal_senders = 5
hot_standby = on
hot_standby_feedback = on
wal_receiver_status_interval = 20s
"""
    hba_additions = "\nlocal replication all trust\n"

    @classmethod
    def start_pg(cls, pgdata):
        run_as_pg(
            "{0}/pg_ctl start -D {1} -o '-c config_file={1}/postgresql.conf' -w".format(
                cls.pgbin, pgdata))

    @classmethod
    def start_pg_master(cls):
        cls.start_pg(cls.pgdata_master)

    @classmethod
    def start_pg_slave(cls):
        cls.start_pg(cls.pgdata_slave)

    @classmethod
    def stop_pg(cls, pgdata):
        run_as_pg("{}/pg_ctl stop -D {} -w".format(cls.pgbin, pgdata))

    @classmethod
    def stop_pg_master(cls):
        cls.stop_pg(cls.pgdata_master)

    @classmethod
    def stop_pg_slave(cls):
        cls.stop_pg(cls.pgdata_slave)

    @classmethod
    def crash_stop_pg(cls, pgdata):
        run_as_pg("{}/pg_ctl stop -D {} -w -m immediate".format(cls.pgbin, pgdata))

    @classmethod
    def cleanup(cls):
        for datadir in (cls.pgdata_master, cls.pgdata_slave):
            try:
                cls.crash_stop_pg(datadir)
            except subprocess.CalledProcessError:
                pass
        run_as_pg("rm -rf {}".format(cls.pgdata_master))
        run_as_pg("rm -rf {}".format(cls.pgdata_slave))

    @classmethod
    def setUpClass(cls):
        cls.cleanup()
        # create master instance
        run_as_pg("mkdir -p -m 0700 {}".format(cls.pgdata_master))
        run_as_pg("{}/initdb -D {}".format(cls.pgbin, cls.pgdata_master))
        with open(cls.pgdata_master + "/postgresql.conf", "a") as fh:
            fh.write(cls.conf_additions.format(cls.pgport_master))
        with open(cls.pgdata_master + "/pg_hba.conf", "a") as fh:
            fh.write(cls.hba_additions)
        cls.start_pg_master()
        # backup master to slave and set it up as a standby
        run_as_pg("mkdir -p -m 0700 {}".format(cls.pgdata_slave))
        run_as_pg("pg_basebackup -D {} -d 'port={}' -Xs".format(
            cls.pgdata_slave, cls.pgport_master))
        run_as_pg(
            "sed -i s/{}/{}/ {}".format(
                cls.pgport_master,
                cls.pgport_slave,
                cls.pgdata_slave + "/postgresql.conf"))
        write_as_pg(cls.pgdata_slave + "/recovery.conf", """\
standby_mode = on
recovery_target_timeline = latest
primary_conninfo = 'port={}'
""".format(cls.pgport_master))
        cls.start_pg_slave()

    # @classmethod
    # def tearDownClass(cls):
    #     cls.cleanup()

    def setUp(self):
        try:
            self.stop_pg_slave()
        except subprocess.CalledProcessError:
            pass
        run_as_pg("rm -f " + get_recovery_tpl(), False)
        os.environ['OCF_RESKEY_bindir'] = self.pgbin
        os.environ['OCF_RESKEY_start_opts'] = '-c config_file={}/postgresql.conf'.format(self.pgdata_slave)
        # os.environ['OCF_RESKEY_pghost'] = "localhost"
        self.set_env_to_slave()

    @classmethod
    def set_env_to_master(cls):
        os.environ['OCF_RESKEY_pgdata'] = cls.pgdata_master
        os.environ['OCF_RESKEY_pgport'] = cls.pgport_master

    @classmethod
    def set_env_to_slave(cls):
        os.environ['OCF_RESKEY_pgdata'] = cls.pgdata_slave
        os.environ['OCF_RESKEY_pgport'] = cls.pgport_slave
        os.environ['OCF_RESOURCE_INSTANCE'] = "foo"

    def test_pg_validate_all(self):
        write_as_pg(get_recovery_tpl(), TestPg.tpl)
        rc = pg_validate_all()
        self.assertEqual(OCF_SUCCESS, rc)

    def test_pgsql_start(self):
        self.assertRaises(SystemExit, pg_start)
        write_as_pg(get_recovery_tpl(), TestPg.tpl)
        self.assertEqual(OCF_SUCCESS, pg_start())
        # check if it is connecting to the master
        self.set_env_to_master()
        self.assertEqual("centos-pg-1", get_connected_standbies()[0].application_name)

    def test_run_pgisready(self):
        rc = run_pgisready()
        self.assertEqual(2, rc)
        self.start_pg_slave()
        rc = run_pgisready()
        self.assertEqual(0, rc)

    def test_confirm_stopped(self):
        self.assertEqual(OCF_NOT_RUNNING, confirm_stopped())
        self.start_pg_slave()
        self.assertEqual(OCF_ERR_GENERIC, confirm_stopped())

    def test_get_pg_ctl_status(self):
        self.assertEqual(3, get_pg_ctl_status())
        self.start_pg_slave()
        self.assertEqual(0, get_pg_ctl_status())
        self.stop_pg_slave()
        self.assertEqual(3, get_pg_ctl_status())

    def test_get_pg_cluster_state(self):
        state = get_pg_cluster_state()
        self.assertEqual('shut down in recovery', state)
        self.start_pg_slave()
        state = get_pg_cluster_state()
        self.assertEqual('in archive recovery', state)
        self.stop_pg_slave()
        state = get_pg_cluster_state()
        self.assertEqual('shut down in recovery', state)

    def test_pg_execute(self):
        self.start_pg_slave()
        rc, rs = pg_execute("SELECT pg_last_xlog_receive_location()")
        self.assertEqual(0, rc)
        rc, rs = pg_execute("SELECT 25")
        self.assertEqual(0, rc)
        self.assertEqual([["25"]], rs)

    def test_get_ocf_status_from_pg_cluster_state(self):
        ocf_status = get_ocf_status()
        self.assertEqual(ocf_status, OCF_NOT_RUNNING)
        self.start_pg_slave()
        ocf_status = get_ocf_status()
        self.assertEqual(ocf_status, OCF_SUCCESS)

    def test_dummy(self):
        self.assertTrue(True)


class TestHa(TestCase):

    uname = subprocess.check_output("uname -n", shell=True).strip()

    def test_get_ha_nodes(self):
        nodes = get_ha_nodes()
        self.assertIn(TestHa.uname, nodes)

    def test_get_ocf_nodename(self):
        n = get_ocf_nodename()
        self.assertEqual(n, TestHa.uname)


class TestRegExes(TestCase):
    tpl = """\
primary_conninfo = 'user=postgres host=127.0.0.1 port=15432 application_name=pc1234.home'
recovery_target_timeline = 'latest'
"""
    cluster_state = """\
Catalog version number:               201608131
Database system identifier:           6403755386726595987
Database cluster state:               in production
pg_control last modified:             Fri 31 Mar 2017 10:01:29 PM CEST
"""

    def test_tpl_file(self):
        m = RE_TPL_TIMELINE.search(TestRegExes.tpl)
        self.assertIsNotNone(m)

    def test_app_name(self):
        m = RE_APP_NAME.findall(TestRegExes.tpl)
        self.assertIsNotNone(m)
        self.assertEqual(m[0], "pc1234.home")

    def test_pg_cluster_state(self):
        m = RE_PG_CLUSTER_STATE.findall(TestRegExes.cluster_state)
        self.assertIsNotNone(m)
        self.assertEqual(m[0], "in production")
