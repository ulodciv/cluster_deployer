import logging
import re
import subprocess
from abc import abstractmethod, ABCMeta
from pathlib import Path
from threading import Lock
from time import sleep, time

from deployer_error import DeployerError


class VmBase(metaclass=ABCMeta):
    start_time = time()
    logger = logging.getLogger()
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    logger.setLevel(logging.DEBUG)

    def __init__(self, *, vm_ova, name, **kwargs):
        super(VmBase, self).__init__(**kwargs)
        self.name = name
        self.vm_ova = vm_ova

    @classmethod
    def class_log(cls, s: str, name=None):
        if name is None:
            name = "global"
        cls.logger.debug(f"{time() - cls.start_time:7.3f} {name}: {s}")

    def log(self, s: str):
        VmBase.class_log(s, self.name)

    @abstractmethod
    def vm_deploy(self, fail_if_exists=True):
        pass

    @abstractmethod
    def vm_start_and_get_ip(self, fail_if_already_running=True):
        pass

    def boot_sleep(self, seconds=15):
        self.log(f"booting or rebooting, sleeping {seconds} seconds...")
        sleep(seconds)
        self.log("done sleeping upon booting or rebooting")


class VmWare(VmBase):
    def __init__(self, **kwargs):
        super(VmWare, self).__init__(**kwargs)

    def vm_deploy(self, fail_if_exists=True):
        pass

    def vm_start_and_get_ip(self, fail_if_already_running=True):
        pass


class Vbox(VmBase):
    IP_PROP = '"/VirtualBox/GuestInfo/Net/0/V4/IP"'
    IP_RE = re.compile(r"[0-9]+(?:\.\d+){3}")
    VM_LIST_RE = re.compile(r'(?:\"(?P<vmname>.*)\")', re.M)
    _vbox_machine_dir = None
    _vbox_ova_disk_units = {}
    _vbox_existing_vms = None
    _vbox_running_vms = None
    vbox_lock = Lock()

    def __init__(self, **kwargs):
        super(Vbox, self).__init__(**kwargs)

    @classmethod
    def _vbox_manage(cls, args, get_stdout, log_func=None):
        cmd = f"vboxmanage {args}"
        if log_func is None:
            cls.class_log(cmd)
        else:
            log_func(cmd)
        try:
            res = subprocess.run(
                cmd,
                shell=True,
                stderr=subprocess.DEVNULL,
                stdout=subprocess.PIPE if get_stdout else subprocess.DEVNULL,
                check=True)
        except subprocess.CalledProcessError as e:
            m = f"error running {cmd}:\n{e}"
            raise DeployerError(m)
        if get_stdout:
            return res.stdout.decode("utf-8")

    @classmethod
    def get_existing_vms(cls):
        with cls.vbox_lock:
            if cls._vbox_existing_vms is None:
                s = cls._vbox_manage(f'list vms', True)
                cls._vbox_existing_vms = cls.VM_LIST_RE.findall(s)
        return cls._vbox_existing_vms

    @classmethod
    def get_running_vms(cls):
        with cls.vbox_lock:
            if cls._vbox_running_vms is None:
                s = cls._vbox_manage(f'list runningvms', True)
                cls._vbox_running_vms = cls.VM_LIST_RE.findall(s)
        return cls._vbox_running_vms

    @classmethod
    def get_disk_unit_of_ova(cls, ova_file):
        ova_file = str(ova_file)
        with cls.vbox_lock:
            if ova_file not in cls._vbox_ova_disk_units:
                s = cls._vbox_manage(f"import {ova_file} -n", True)
                p = re.compile(r'(?P<n>\d+): Hard disk', re.M)
                cls._vbox_ova_disk_units[ova_file] = p.findall(s)[0]
        return cls._vbox_ova_disk_units[ova_file]

    @classmethod
    def get_vbox_machine_dir(cls):
        with cls.vbox_lock:
            if cls._vbox_machine_dir is None:
                s = cls._vbox_manage("list systemproperties", True)
                p = re.compile(r'Default machine folder:\s*(?P<v>.*)')
                cls._vbox_machine_dir = Path(p.findall(s)[0].strip())
        return cls._vbox_machine_dir

    def vbox_manage(self, args, get_stdout):
        return Vbox._vbox_manage(args, get_stdout, self.log)

    def vm_deploy(self, fail_if_exists=True):
        name = self.name
        if name in Vbox.get_existing_vms():
            if not fail_if_exists:
                return
            raise DeployerError(f"vm {name} already exists")
        vmdk = Vbox.get_vbox_machine_dir() / name / f"{name}.vmdk"
        self.vbox_manage(
            (f'import {self.vm_ova} --vsys 0 --vmname {name} '
             f'--unit {Vbox.get_disk_unit_of_ova(self.vm_ova)} --disk "{vmdk}"'),
            False)

    def _del_ip_property(self):
        self.vbox_manage(
            f'guestproperty delete {self.name} {self.IP_PROP}', False)

    def vm_start_and_get_ip(self, fail_if_already_running=True):
        already_running = self.name in Vbox.get_running_vms()
        if already_running and fail_if_already_running:
            raise DeployerError(f"vm {self.name} already running")
        if not already_running:
            self._del_ip_property()
            self.vbox_manage(f'startvm {self.name}', False)
            self.boot_sleep()
        self._get_ip()
        self.log(f"vm {self.name} is running with ip {self.ip}")

    def _get_ip_property(self):
        # b'Value: 192.168.179.10\r\n'
        s = self.vbox_manage(
            f'guestproperty get {self.name} {self.IP_PROP}', True)
        if ":" not in s:
            return None
        self.ip = self.IP_RE.findall(s)[0]

    def _get_ip(self, max_tries=30, sleep_secs=2):
        tries = 0
        while tries < max_tries:
            tries += 1
            self._get_ip_property()
            if self.ip:
                return
            sleep(sleep_secs)
        raise DeployerError(
            f"failed to get ip of vm {self.name} after trying {tries} "
            f"with {sleep_secs}s between each attempt")
