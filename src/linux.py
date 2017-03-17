import re
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from contextlib import contextmanager
from enum import Enum, auto
from ipaddress import IPv4Interface
from pathlib import PurePosixPath
from time import sleep

from hosts_file import CentOsConfigFile
from vm import VmBase


class Distro(Enum):
    UNKNOWN = auto()
    CENTOS = auto()
    UBUNTU = auto()
    DEBIAN = auto()


class LinuxUser:
    def __init__(self, user):
        self.user = user
        self.home_dir = None
        self.public_ssh_key = None


class Linux(VmBase, metaclass=ABCMeta):

    def __init__(self, *, static_ip, root_password, users, **kwargs):
        super(Linux, self).__init__(**kwargs)
        self._iface = None
        self._selinux_is_active = None
        self._distro = None
        self.root_password = root_password
        self.ip = None
        self.static_ip = static_ip
        self.users = OrderedDict({"root": LinuxUser("root")})
        for user in users:
            if user == "root":
                continue
            self.users[user] = LinuxUser(user)
        self._added_pub_key = False
        self.cluster_vip = None

    @abstractmethod
    def authorize_pub_key_for_root(self):
        pass

    @abstractmethod
    def ssh_run(self, command_or_commands, user="root", check=False):
        pass

    @abstractmethod
    def ssh_run_(self, user, ssh, command, check):
        pass

    @abstractmethod
    @contextmanager
    def open_sftp(self, user="root"):
        pass

    @abstractmethod
    def ssh_run_check(self, command_or_commands, user="root"):
        pass

    @abstractmethod
    @contextmanager
    def ssh_root_with_password(self):
        pass

    @abstractmethod
    @contextmanager
    def open_ssh(self, user="root"):
        pass

    @abstractmethod
    def authorize_pub_key(self, user_obj):
        pass

    def get_os_release(self):
        os_release_file = PurePosixPath("/etc/os-release")
        with self.open_sftp() as sftp:
            with sftp.file(str(os_release_file)) as f:
                return f.read().decode('utf-8')

    @property
    def distro(self):
        if self._distro is None:
            s = self.get_os_release()
            p = re.compile(r'^ID="?(?P<distro>\w+)"?', re.M)
            self._distro = Distro[p.findall(s)[0].upper()]
        return self._distro

    def reboot(self):
        self.ssh_run("systemctl reboot")

    def set_hostname(self):
        self.ssh_run_check(f"hostnamectl set-hostname {self.name}")

    def assume_ip_is_static(self):
        self.ip = self.static_ip

    def _user_exists(self, user):
        with self.ssh_root_with_password() as ssh:
            i, o, e = ssh.exec_command(f"id -u {user}")
            return o.channel.recv_exit_status() == 0

    def add_user(self, user, pwd=None):
        with self.ssh_root_with_password() as ssh:
            self.ssh_run_("root", ssh, f"useradd -m {user}", True)
            if not pwd:
                return
            self.ssh_run_(
                "root",
                ssh,
                (f"chpasswd << END\n"
                 f"{user}:{pwd}\n"
                 f"END"),
                True)

    def _get_user_home_dir(self, user_obj):
        with self.ssh_root_with_password() as ssh:
            o, e = self.ssh_run_(
                "root",
                ssh,
                f"getent passwd {user_obj.user}",
                True)
            user_obj.home_dir = o.read().decode('utf-8').split(":")[5]

    def selinux_is_active(self):
        if self._selinux_is_active is None:
            o, e = self.ssh_run_check("getenforce")
            self._selinux_is_active = o.read().decode('utf-8').strip().lower() == "enforcing"
        return self._selinux_is_active

    def setup_users(self):
        """
        for each user: 
            - add if it doesn't exists
            - add this app's SSH pub key if it doesn't have it 
              (and test SSH acccess)
            - add an RSA key pair if it lacks one
        for each user: 
            - provide per user cross-cluster key SSH authentication 
        """
        root_obj = self.users["root"]
        self._get_user_home_dir(root_obj)
        self.authorize_pub_key_for_root()
        for user in self.users:
            if user == "root":
                continue
            user_obj = self.users[user]
            if not self._user_exists(user):
                self.add_user(user)
            self._get_user_home_dir(user_obj)
            self.authorize_pub_key(user_obj)

    def delete_user(self, user: str, remove_dir=True):
        if remove_dir:
            self.ssh_run_check(f"userdel -r {user}")
        else:
            self.ssh_run_check(f"userdel {user}")

    @property
    def iface(self):
        if self._iface is None:
            with self.open_ssh() as ssh:
                stdin, stdout, stderr = ssh.exec_command("ip a")
                p = re.compile(
                    rf"""
                    inet\s{self.ip}/\d+\s
                    brd\s\d+\.\d+\.\d+\.\d+\s
                    scope\sglobal\s(?:dynamic\s)?(?P<iface>\w+)
                    """,
                    re.X | re.M)
                s = stdout.read()
                self._iface = p.findall(s.decode("utf-8"))[0]
        return self._iface

    def add_temp_ipv4_to_iface(self, ipv4: IPv4Interface):
        self.ssh_run_check(
            f"ip addr add {ipv4.ip}/{ipv4.network.prefixlen} dev {self.iface}")

    def del_temp_ipv4_to_iface(self, ipv4: IPv4Interface):
        self.ssh_run_check(
            f"ip addr del {ipv4.ip}/{ipv4.network.prefixlen} dev {self.iface}")

    def _tune_interfaces(self, cfg: str) -> str:
        res = ""
        for l in cfg.splitlines():
            if l.startswith(f"iface {self.iface} inet dhcp"):
                res += (
                    f"auto {self.iface}\n"
                    f"iface {self.iface} inet static\n"
                    f"    address {self.static_ip}\n"
                    f"    gateway {self.static_ip_gw}\n")
            else:
                res += f"{l}\n"
        return res

    @property
    def static_ip_gw(self):
        return self.static_ip.rpartition(".")[0] + ".1"

    def set_static_ip(self):
        cfg_str_new = None
        cfg_file = None
        distro = self.distro
        if distro == Distro.CENTOS:
            cfg_file = f"/etc/sysconfig/network-scripts/ifcfg-{self.iface}"
        elif distro in (Distro.UBUNTU, Distro.DEBIAN):
            cfg_file = f"/etc/network/interfaces"
        with self.open_sftp() as sftp:
            with sftp.file(cfg_file, "r") as f:
                cfg_str = f.read().decode("utf-8")
            if distro == Distro.CENTOS:
                cfg = CentOsConfigFile(cfg_str)
                cfg.make_ifcfg_ip_static(self.static_ip, self.static_ip_gw)
                cfg_str_new = str(cfg)
            elif distro in (Distro.UBUNTU, Distro.DEBIAN):
                cfg_str_new = self._tune_interfaces(cfg_str)
            if cfg_str_new == cfg_str:
                self.log(f"ip {self.ip} is already static and correct, "
                         f"nothing to change")
                return
            self.log(f"ip being changed from {self.ip} "
                     f"to static {self.static_ip}")
            with sftp.file(cfg_file, "w") as f:
                f.write(cfg_str_new)
        if distro == Distro.CENTOS:
            self.ssh_run("systemctl restart network")
            sleep_for = 5
        else:   # distro in (Distro.UBUNTU, Distro.DEBIAN):
            sleep_for = 15
            # self.ssh_run("systemctl restart networking")
            self.reboot()
        self.ip = self.static_ip
        self.log(f"sleeping {sleep_for} seconds...")
        sleep(sleep_for)
        self.log(f"slept {sleep_for} seconds")
        self.log(f"testing ssh connection to {self.ip}...")
        with self.open_ssh() as _:
            self.log(f"connection sucessful, IP is now {self.ip}")
