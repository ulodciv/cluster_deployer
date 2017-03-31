import subprocess
from unittest import TestCase

from pgsqlms2 import (_query, RE_TPL_TIMELINE, RE_APP_NAME, ocf_local_nodename)


tpl = """\
primary_conninfo = 'user=postgres host=127.0.0.1 port=15432 application_name=pc1234.home'
recovery_target_timeline = 'latest'
"""


class TestPgStuff(TestCase):
    pg_data_dir = "/tmp/pgdata_test_pgsqlms2"
    pg_bin = "/usr/pgsql-9.6/bin"

    @classmethod
    def check_call(cls, c):
        print(c)
        subprocess.check_call(c, shell=True)

    @classmethod
    def setUpClass(cls):
        # sudo -iu postgres mkdir -p $PGDATA
        # sudo -iu postgres $PGBIN/initdb --nosync -D $PGDATA &> /dev/null
        # sudo -iu postgres "$PGBIN"/pg_ctl -D "$PGDATA" -w start &> /dev/null
        c = "sudo -iu postgres mkdir -p {}".format(cls.pg_data_dir)
        cls.check_call(c)
        c = "sudo -iu postgres {}/initdb --nosync -D {} &> /dev/null".format(
                cls.pg_bin, cls.pg_data_dir)
        cls.check_call(c)
        c = "sudo -iu postgres {}/pg_ctl -D {} -w start &> /dev/null".format(
                cls.pg_bin, cls.pg_data_dir)
        cls.check_call(c)
        c = "sudo -iu postgres rm -rf {}".format(cls.pg_data_dir)
        cls.check_call(c)

    def test_1(self):
        self.assertTrue(True)

    @classmethod
    def tearDownClass(cls):
        c = "sudo -iu postgres {}/pg_ctl -D {} -w -m immediate stop  &> /dev/null".format(
                cls.pg_bin, cls.pg_data_dir)
        cls.check_call(c)


class TestOcfFunctions(TestCase):
    def test_ocf_local_nodename(self):
        n = ocf_local_nodename()
        self.assertEqual(n, 'centos-pg-1')


class TestRegEx(TestCase):
    def test_tpl_file(self):
        m = RE_TPL_TIMELINE.search(tpl)
        self.assertIsNotNone(m)

    def test_app_name(self):
        m = RE_APP_NAME.findall(tpl)
        self.assertIsNotNone(m)
        self.assertEqual(m[0], "pc1234.home")


# class TestPgStuff(TestCase):
#     def test_query_1(self):
#         rc, rs = _query("SELECT pg_last_xlog_receive_location()")

