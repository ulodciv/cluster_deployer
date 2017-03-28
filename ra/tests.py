from unittest import TestCase

from pgsqlms import (_query, RE_TPL_TIMELINE, RE_APP_NAME, ocf_local_nodename)


tpl = """\
primary_conninfo = 'user=postgres host=127.0.0.1 port=15432 application_name=pc1234.home'
recovery_target_timeline = 'latest'
"""


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

