#!/usr/bin/env python3.6
from unittest import TestCase

from deployer_error import DeployerError
from hosts_file import HostsFile, CentOsConfigFile

HOSTS1 = """\
127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4
::1         localhost localhost.localdomain localhost6 localhost6.localdomain6
192.168.72.101 pg01
192.168.72.102 pg02
"""

IFCFG1 = """\
TYPE=Ethernet
BOOTPROTO=static
DEFROUTE=yes
IPV4_FAILURE_FATAL=no
IPV6INIT=yes
IPV6_AUTOCONF=yes
IPV6_DEFROUTE=yes
IPV6_FAILURE_FATAL=no
IPV6_ADDR_GEN_MODE=stable-privacy
NAME=enp0s3
UUID=b1a902cc-45ec-4b28-aac1-f81500da526c
DEVICE=enp0s3
ONBOOT=yes
PEERDNS=yes
PEERROUTES=yes
IPV6_PEERDNS=yes
IPV6_PEERROUTES=yes
IPV6_PRIVACY=no
ZONE=public
IPADDR=192.168.72.101
"""

IFCFG_BEFORE = """\
TYPE=Ethernet
BOOTPROTO=dhcp
DEFROUTE=yes
IPV4_FAILURE_FATAL=no
IPV6INIT=yes
IPV6_AUTOCONF=yes
IPV6_DEFROUTE=yes
IPV6_FAILURE_FATAL=no
IPV6_ADDR_GEN_MODE=stable-privacy
NAME=enp0s3
UUID=b1a902cc-45ec-4b28-aac1-f81500da526c
DEVICE=enp0s3
ONBOOT=yes
PEERDNS=yes
PEERROUTES=yes
IPV6_PEERDNS=yes
IPV6_PEERROUTES=yes
IPV6_PRIVACY=no
ZONE=public
"""

IFCFG_AFTER = """\
TYPE=Ethernet
BOOTPROTO=static
DEFROUTE=yes
IPV4_FAILURE_FATAL=no
IPV6INIT=no
NAME=enp0s3
UUID=b1a902cc-45ec-4b28-aac1-f81500da526c
DEVICE=enp0s3
ONBOOT=yes
PEERDNS=yes
PEERROUTES=yes
ZONE=public
IPADDR=192.168.72.101
"""


class TestRedhatConfigFile(TestCase):

    def test_parse_simple(self):
        cfg = CentOsConfigFile(IFCFG1)
        self.assertEqual(cfg["IPV6_DEFROUTE"], "yes")

    def test_recompose(self):
        cfg = CentOsConfigFile(IFCFG1)
        self.assertEqual(str(cfg), IFCFG1)

    def test_one_change(self):
        cfg = CentOsConfigFile(IFCFG1)
        self.assertEqual(cfg["IPV6INIT"], "yes")
        cfg["IPV6INIT"] = "no"
        self.assertNotEqual(str(cfg), IFCFG1)
        self.assertEqual(cfg["IPV6INIT"], "no")

    def test_changes(self):
        cfg = CentOsConfigFile(IFCFG_BEFORE)
        cfg.make_ifcfg_ip_static("192.168.72.101")
        self.assertEqual(str(cfg), IFCFG_AFTER)


class TestHostsFile(TestCase):

    def test_parse_simple(self):
        hosts = HostsFile(HOSTS1)
        self.assertEqual(hosts.count(), 4)
        self.assertEqual(
            1, len([e for e in hosts.entries if e.entry_type == "ipv6"]))
        self.assertEqual(
            3, len([e for e in hosts.entries if e.entry_type == "ipv4"]))
        self.assertEqual(
            0, len([e for e in hosts.entries if e.entry_type == "blank"]))
        self.assertEqual(
            0, len([e for e in hosts.entries if e.entry_type == "comment"]))
        self.assertEqual(len(hosts.entries[0].names), 4)
        self.assertEqual(len(hosts.entries[1].names), 4)
        self.assertEqual(len(hosts.entries[2].names), 1)
        self.assertEqual(len(hosts.entries[3].names), 1)
        self.assertEqual(
            hosts.entries[1].names,
            ["localhost",
             "localhost.localdomain",
             "localhost6",
             "localhost6.localdomain6"])
        as_str = f"{hosts}"
        self.assertEqual(len(as_str.splitlines()), 4)
        self.assertEqual(
            as_str.splitlines()[2].split(),
            ["192.168.72.101", "pg01"])

    def test_remove_address(self):
        hosts = HostsFile(HOSTS1)
        self.assertEqual(hosts.count(), 4)
        self.assertTrue(hosts.has_address("192.168.72.101"))
        self.assertTrue(hosts.has_name("pg01"))
        hosts.remove_adress("192.168.72.101")
        self.assertEqual(hosts.count(), 3)
        self.assertFalse(hosts.has_address("192.168.72.101"))
        self.assertFalse(hosts.has_name("pg01"))

    def test_add_host_address_there(self):
        hosts = HostsFile(HOSTS1)
        self.assertEqual(hosts.count(), 4)
        self.assertFalse(hosts.has_name("pg25"))
        hosts.add_or_update("192.168.72.101", "pg25")
        self.assertEqual(hosts.count(), 4)
        self.assertTrue(hosts.has_name("pg25"))
        self.assertEqual(hosts.entries[2].names, ["pg01", "pg25"])

    def test_add_host_new_address(self):
        hosts = HostsFile(HOSTS1)
        self.assertEqual(hosts.count(), 4)
        hosts.add_or_update("192.168.72.151", "pg35")
        self.assertEqual(hosts.count(), 5)
        self.assertEqual(hosts.entries[-1].names, ["pg35"])

    def test_add_name_already_there(self):
        hosts = HostsFile(HOSTS1)
        self.assertEqual(hosts.count(), 4)
        with self.assertRaises(DeployerError):
            hosts.add_or_update("192.168.72.151", "pg01")
