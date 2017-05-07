import re
from collections import OrderedDict
from ipaddress import ip_address, IPv4Address, IPv6Address

from deployer_error import DeployerError


class CentOsConfigFile(OrderedDict):
    def __init__(self, file_str: str):
        super(CentOsConfigFile, self).__init__()
        self.file_str = file_str
        for l in file_str.splitlines():
            k, _, v = l.partition("=")
            self[k] = v

    def __str__(self):
        return "".join(
            f"{k}={v}\n" for k, v in self.items())

    def make_ifcfg_ip_static(self, ip, gw=None):
        ipv6keys = [k for k in self if k.startswith("IPV6_")]
        for k in ipv6keys:
            del self[k]
        if "IPV6INIT" in self:
            self["IPV6INIT"] = "no"
        self["IPADDR"] = str(ip)
        # self["GATEWAY"] = str(gw)
        self["BOOTPROTO"] = "static"


class HostsEntry:
    HOSTNAME_RE = re.compile('(?!-)[A-Z\d-]{1,63}(?<!-)$', re.IGNORECASE)

    def __init__(self, hosts_line: str):
        self.address = None
        self.comment = None
        self.names = None
        self.line = hosts_line.strip()
        if not self._parse_line():
            raise DeployerError(
                f"failed to parse hosts file line:\n"
                f"{hosts_line}")

    def _parse_line(self):
        if not self.line:
            self.entry_type = 'blank'
            return True
        if self.line.startswith("#"):
            self.entry_type = 'comment'
            self.comment = self.line
            return True
        parts = self.line.split()
        self.address = ip_address(parts[0])
        if type(self.address) is IPv4Address:
            self.entry_type = 'ipv4'
        elif type(self.address) is IPv6Address:
            self.entry_type = 'ipv6'
        else:
            return False
        self.names = parts[1:]
        return self._names_are_ok()

    @property
    def has_address(self):
        return self.address is not None

    def _names_are_ok(self):
        for name in self.names:
            if len(name) > 255:
                return False
            if not all(self.HOSTNAME_RE.match(x) for x in name.split(".")):
                return False
        return True

    def __str__(self):
        if self.entry_type == 'comment':
            return self.comment
        elif self.entry_type == 'blank':
            return ""
        else:  # self.entry_type in ('ipv4', 'ipv6'):
            return f"{self.address}\t{' '.join(self.names)}"


class HostsFile:
    def __init__(self, hosts_str: str):
        self.entries = []
        self.hosts_str = hosts_str
        for line in self.hosts_str.splitlines():
            self.entries.append(HostsEntry(line))

    def __str__(self):
        return "".join(str(e) + "\n" for e in self.entries)

    def count(self):
        return len(self.entries)

    def get_hosts_as_str(self):
        return str(self)

    def addresses_of_name(self, name):
        entries = [e for e in self.iter_real_entries() if name in e.names]
        return [e.address for e in entries]

    def has_name(self, name):
        return any(name in e.names for e in self.iter_real_entries())

    def has_address(self, address):
        addr = ip_address(address)
        return any(addr == e.address for e in self.iter_real_entries())

    def iter_real_entries(self):
        for entry in self.entries:
            if entry.address:
                yield entry

    def remove_name(self, name):
        entries = [e for e in self.iter_real_entries() if name in e.names]
        for entry in entries:
            entry.names.remove(name)
            if not entry.names:
                self.entries.remove(entry)

    def remove_adress(self, address):
        address = ip_address(address)
        entries = [e for e in self.iter_real_entries() if address == e.address]
        for entry in entries:
            self.entries.remove(entry)

    def add_or_update(self, address, name):
        if self.has_name(name):
            raise DeployerError("name already in hosts file")
        address = ip_address(address)
        entries = [e for e in self.iter_real_entries() if e.address == address]
        if entries:
            if address == entries[0].address:
                if name in entries[0].names:
                    # case 1: address and name already there
                    return
                # case 2: address already there without name
                entries[0].names.append(name)
                return
        # case 3: address not there
        self.entries.append(HostsEntry(f"{address} {name}"))
