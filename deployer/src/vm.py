import logging
import re
import shlex
from subprocess import run, PIPE
from abc import abstractmethod, ABCMeta
from pathlib import Path
from threading import Lock
from time import sleep

from deployer_error import DeployerError


class VmBase(metaclass=ABCMeta):
    def __init__(self, *, ova, name, **kwargs):
        super(VmBase, self).__init__()
        self.name = name
        self.ova = ova
        self.logger = logging.getLogger(self.name)

    def log(self, msg: str):
        self.logger.debug(msg)

    @abstractmethod
    def vm_deploy(self, fail_if_exists=True):
        pass

    @abstractmethod
    def vm_start_and_get_ip(self, fail_if_already_running=True):
        pass

    def boot_sleep(self, seconds=10):
        self.log(f"sleeping {seconds} seconds...")
        sleep(seconds)
        self.log("done sleeping")


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

    def __init__(self, *, vboxmanage, **kwargs):
        super(Vbox, self).__init__(**kwargs)
        self.vboxmanage = vboxmanage

    def run_vboxmanage(self, args):
        if type(args) is str:
            cmd = [self.vboxmanage, *shlex.split(args)]
        else:
            cmd = [self.vboxmanage, *args]
        self.log(" ".join(cmd))
        res = run(cmd, stdout=PIPE, stderr=PIPE)
        if res.returncode:
            raise DeployerError(f"error running {cmd}:\n{res.stderr}")
        return res.stdout.decode()

    def get_existing_vms(self):
        with Vbox.vbox_lock:
            if Vbox._vbox_existing_vms is None:
                s = self.run_vboxmanage('list vms')
                Vbox._vbox_existing_vms = Vbox.VM_LIST_RE.findall(s)
        return Vbox._vbox_existing_vms

    def get_running_vms(self):
        with Vbox.vbox_lock:
            if Vbox._vbox_running_vms is None:
                s = self.run_vboxmanage(f'list runningvms')
                Vbox._vbox_running_vms = Vbox.VM_LIST_RE.findall(s)
        return Vbox._vbox_running_vms

    def get_disk_unit_of_ova(self, ova_file):
        ova_file = str(ova_file)
        with Vbox.vbox_lock:
            if ova_file not in Vbox._vbox_ova_disk_units:
                s = self.run_vboxmanage(f"import {ova_file} -n")
                p = re.compile(r'(?P<n>\d+): Hard disk', re.M)
                Vbox._vbox_ova_disk_units[ova_file] = p.findall(s)[0]
        return Vbox._vbox_ova_disk_units[ova_file]

    def get_vbox_machine_dir(self):
        with Vbox.vbox_lock:
            if Vbox._vbox_machine_dir is None:
                s = self.run_vboxmanage("list systemproperties")
                p = re.compile(r'Default machine folder:\s*(?P<v>.*)')
                Vbox._vbox_machine_dir = Path(p.findall(s)[0].strip())
        return Vbox._vbox_machine_dir

    def vm_deploy(self, fail_if_exists=True):
        name = self.name
        if name in self.get_existing_vms():
            if not fail_if_exists:
                return
            raise DeployerError(f"vm {name} already exists")
        vmdk = self.get_vbox_machine_dir() / name / f"{name}.vmdk"
        self.run_vboxmanage(
            f'import {self.ova} --vsys 0 --vmname {name} --unit '
            f'{self.get_disk_unit_of_ova(self.ova)} --disk "{vmdk}"')

    def _del_ip_property(self):
        self.run_vboxmanage(f'guestproperty delete {self.name} {self.IP_PROP}')

    def vm_start_and_get_ip(self, fail_if_already_running=True):
        already_running = self.name in self.get_running_vms()
        if already_running and fail_if_already_running:
            raise DeployerError(f"vm {self.name} already running")
        if not already_running:
            self._del_ip_property()
            self.run_vboxmanage(f'startvm {self.name}')
            self.boot_sleep()
        self._get_ip()
        self.log(f"vm {self.name} is running with ip {self.ip}")

    def _get_ip_property(self):
        # b'Value: 192.168.179.10\r\n'
        s = self.run_vboxmanage(f'guestproperty get {self.name} {self.IP_PROP}')
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
