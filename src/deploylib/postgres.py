import re
from abc import ABCMeta
from collections import OrderedDict
from pathlib import PurePosixPath
from threading import Lock
from time import sleep

from .deployer_error import DeployerError
from .linux import Distro
from .ssh import Ssh


class PgConfigReader(OrderedDict):
    RE_COMMENT = re.compile(r"\s*#.*")
    RE_EMPTY = re.compile(r"\s*")
    RE_SETTING = re.compile(r"\s*(\S*)(\s*=\s*|\s*)(.*)\s*")
    RE_VALUE = re.compile("(?:(\'.*\')(?:.*))|(?:([^'][^#^\s]*)(?:\s+#.*)?)")

    def __init__(self, postgresql_conf: str, *args, **kwds):
        super().__init__(*args, **kwds)
        for line in postgresql_conf.splitlines():
            self._parse_conf_line(line)

    def update_with_auto_conf(self, auto_conf: str):
        for line in auto_conf.splitlines():
            self._parse_conf_line(line)

    def _parse_conf_line(self, line):
        if self.RE_COMMENT.fullmatch(line) or self.RE_EMPTY.fullmatch(line):
            return
        m = self.RE_SETTING.fullmatch(line)
        if not m:
            raise DeployerError(f"can't parse this line:\n{line}")
        k, _, value_part = m.groups()
        m = self.RE_VALUE.fullmatch(value_part)
        if not m:
            raise DeployerError(
                f"can't parse value {value_part} from this line:\n{line}")
        val1, val2 = m.groups()
        self[k] = val1.strip("'") if val1 else val2


class Postgres(Ssh, metaclass=ABCMeta):
    """
    config_file (postgresql.conf):
        if not provided:
            go with distro defaults

    data_directory:
        if not provided:
            grab it from postgresql.conf
            if not there
                go with distro defaults

    hba_file:
        grab it from postgresql.conf
        if not there
            go with data_directory/pg_hba.conf
    """
    pg_lock = Lock()

    def __init__(self, *,
                 pg_version,
                 pg_port="5432",
                 pg_user="postgres",
                 pg_repl_user,
                 pg_config_file=None,
                 pg_data_directory=None,
                 pg_bindir=None,
                 **kwargs):
        super(Postgres, self).__init__(**kwargs)
        self.pg_version = pg_version
        self.pg_user = pg_user
        self.pg_repl_user = pg_repl_user
        self.pg_slot = self.name.replace('-', '_')
        self.pg_port = pg_port
        self._pg_service = None
        self._pg_data_directory = pg_data_directory
        self._pg_hba_file = None
        self._pg_config_file = pg_config_file
        self._pg_bindir = pg_bindir
        self._pg_start_opts = None
        self._pg_recovery_file = None  # recovery.conf
        self._pg_pcmk_recovery_file = None  # recovery.conf.pcmk
        self._pg_conf_dict = None

    @property
    def pg_conf_dict(self):
        if self._pg_conf_dict is None:
            # parse postgresql.conf
            with self.open_sftp(self.pg_user) as sftp:
                conf_str = sftp.file(self.pg_config_file).read().decode()
            self._pg_conf_dict = PgConfigReader(conf_str)
            # update conf dict with postgresql.auto.conf
            autoconf_file = PurePosixPath(self.pg_data_directory) / "postgresql.auto.conf"
            with self.open_sftp(self.pg_user) as sftp:
                try:
                    autoconf_str = sftp.file(str(autoconf_file)).read().decode()
                    self._pg_conf_dict.update_with_auto_conf(autoconf_str)
                except FileNotFoundError:
                    pass
        return self._pg_conf_dict

    @property
    def pg_config_file(self):
        """
        if not provided:
            go with distro defaults
        """
        if self._pg_config_file is None:
            distro = self.distro
            ver = self.pg_version
            if distro == Distro.CENTOS:
                self._pg_config_file = (
                    f"/var/lib/pgsql/{ver}/data/postgresql.conf")
            elif distro in (Distro.UBUNTU, Distro.DEBIAN):
                self._pg_config_file = (
                    f"/etc/postgresql/{ver}/main/postgresql.conf")
        return self._pg_config_file

    @property
    def pg_data_directory(self) -> str:
        """ data_directory:
        if not provided:
            grab it from postgresql.conf
            if not there
                go with distro defaults
        """
        if self._pg_data_directory is None:
            if "data_directory" in self.pg_conf_dict:
                self._pg_data_directory = self.pg_conf_dict["data_directory"]
            else:
                distro = self.distro
                ver = self.pg_version
                if distro == Distro.CENTOS:
                    self._pg_data_directory = f"/var/lib/pgsql/{ver}/data"
                elif distro in (Distro.UBUNTU, Distro.DEBIAN):
                    self._pg_data_directory = f"/var/lib/postgresql/{ver}/main"
        return self._pg_data_directory

    @property
    def pg_hba_file(self):
        """ hba_file:
        grab it from postgresql.conf
        if not there
            go with data_directory/pg_hba.conf
        """
        if self._pg_hba_file is None:
            if "hba_file" in self.pg_conf_dict:
                self._pg_hba_file = self.pg_conf_dict["hba_file"]
            else:
                self._pg_hba_file = str(PurePosixPath(self.pg_data_directory) / "pg_hba.conf")
        return self._pg_hba_file

    @property
    def pg_service(self):
        if self._pg_service is None:
            distro = self.distro
            if distro == Distro.CENTOS:
                self._pg_service = f"postgresql-{self.pg_version}"
            elif distro in (Distro.UBUNTU, Distro.DEBIAN):
                self._pg_service = f"postgresql"
        return self._pg_service

    @property
    def pg_start_opts(self):
        if self._pg_start_opts is None:
            distro = self.distro
            if distro == Distro.CENTOS:
                self._pg_start_opts = ""
            elif distro in (Distro.UBUNTU, Distro.DEBIAN):
                self._pg_start_opts = (
                    f"-c config_file="
                    f"/etc/postgresql/{self.pg_version}/main/postgresql.conf")
        return self._pg_start_opts

    @property
    def pg_bindir(self):
        if self._pg_bindir is None:
            distro = self.distro
            if distro == Distro.CENTOS:
                self._pg_bindir = f"/usr/pgsql-{self.pg_version}/bin"
            elif distro in (Distro.UBUNTU, Distro.DEBIAN):
                self._pg_bindir = (
                    f"/usr/lib/postgresql/{self.pg_version}/bin")
        return self._pg_bindir

    @property
    def pg_recovery_file(self):
        if self._pg_recovery_file is None:
            self._pg_recovery_file = PurePosixPath(
                self.pg_data_directory) / "recovery.conf"
        return self._pg_recovery_file

    @property
    def pg_pcmk_recovery_file(self):
        if self._pg_pcmk_recovery_file is None:
            self._pg_pcmk_recovery_file = PurePosixPath(
                self.pg_data_directory) / "recovery.conf.pcmk"
        return self._pg_pcmk_recovery_file

    def pg_current_setting2(self, setting) -> str:
        c = (f'psql -p {self.pg_port} -t '
             f'-c "select current_setting(\'{setting}\');"')
        o = self.ssh_run_check(c, user=self.pg_user, get_output=True)
        return o.strip()

    def pg_current_setting(self, setting):
        ret = None
        tries = 0
        while not ret:
            tries += 1
            ret = self.pg_current_setting2(setting)
            if ret:
                break
            self.log(f"failed to get {setting}, will try again in two seconds")
            sleep(2)
            if tries > 30:
                raise DeployerError(f'waited too long after {setting} setting')
        return ret

    def pg_set_param(self, param, val=None):
        if val is None:
            c = f"alter system reset {param}"
        else:
            c = f"alter system set {param} to {val}"
        self.pg_execute(c)

    def _pg_add_to_conf(self, param, val, conf_file=None):
        if conf_file is None:
            conf_file = self.pg_config_file
        self.ssh_run_check(
            f'echo "{param} = \'{val}\'" >> {conf_file}',
            user=self.pg_user)

    def _pg_add_to_recovery_conf(self, param, val):
        self._pg_add_to_conf(param, val, self.pg_recovery_file)

    def _pg_add_to_pcmk_recovery_conf(self, param, val):
        self._pg_add_to_conf(param, val, self.pg_pcmk_recovery_file)

    def pg_execute(self, sql, *, db="postgres"):
        rs, rs_bash = chr(30), r'\036'  # record separator
        fs, fs_bash = chr(3), r'\003'  # end of text
        cmd = ["psql", "-p", self.pg_port, "-d", db, "-v", "ON_ERROR_STOP=1",
               "-qXAt", "-R", rs_bash, "-F", fs_bash, "-c", f'"{sql};"']
        o = self.ssh_run_check(
            " ".join(cmd), user=self.pg_user, get_output=True)
        if o:
            ans = o[:-1]
            res = []
            for record in ans.split(rs):
                res.append(record.split(fs))
            self.log(f"pg_execute results:\n{res}")
            return res

    def pg_isready(self):
        exe = str(PurePosixPath(self.pg_bindir) / "pg_isready")
        try:
            self.ssh_run_check(f"{exe} -p {self.pg_port}", user=self.pg_user)
            return True
        except DeployerError:
            return False

    def pg_get_server_pid(self):
        pid_file = PurePosixPath(self.pg_data_directory) / "postmaster.pid"
        with self.open_sftp(self.pg_user) as sftp:
            s = sftp.file(str(pid_file)).read()
        if not s:
            return None
        return s.splitlines()[0].strip().decode()

    def pg_create_db(self, db: str):
        self.ssh_run_check(f"createdb -p {self.pg_port} {db}",
                           user=self.pg_user)

    def pg_drop_db(self, db: str):
        self.ssh_run(f"dropdb -p {self.pg_port} {db}", user=self.pg_user)

    def pg_create_replication_user(self):
        self.ssh_run_check(
            f'createuser -p {self.pg_port} {self.pg_repl_user} --replication',
            user=self.pg_user)

    def pg_make_master(self, all_hosts):
        self.pg_set_param("log_min_messages", "DEBUG5")
        self.pg_set_param("wal_level", "replica")
        self.pg_set_param("max_wal_senders", len(all_hosts) + 5)
        self.pg_set_param("max_replication_slots", len(all_hosts) + 5)
        self.pg_set_param("listen_addresses", "'*'")
        self.pg_set_param("hot_standby", "on")
        self.pg_set_param("hot_standby_feedback", "on")
        self.pg_set_param("wal_log_hints", "on")
        self.pg_set_param("wal_receiver_timeout", "3000")  # ms
        self.pg_set_param("wal_retrieve_retry_interval", "500")  # ms
        # for Debian and Ubuntu
        self.pg_set_param("stats_temp_directory", "pg_stat_tmp")

    def pg_write_recovery_for_pcmk(self, master_host):
        repl_user = self.pg_repl_user
        self._pg_add_to_pcmk_recovery_conf("standby_mode", "on")
        self._pg_add_to_pcmk_recovery_conf("recovery_target_timeline", "latest")
        self._pg_add_to_pcmk_recovery_conf(
            "primary_conninfo",
            f"host={master_host} port={self.pg_port} user={repl_user}")
        self._pg_add_to_pcmk_recovery_conf("primary_slot_name", self.pg_slot)

    def pg_backup(self, master: 'Postgres'):
        self.ssh_run_check([
            f"mv {master.pg_data_directory} data_old",
            f"pg_basebackup -h {master.name} -p {self.pg_port} "
            f"-D {master.pg_data_directory} -U {self.pg_repl_user} -Xs"],
            user=self.pg_user)

    def pg_create_user(self, user, super_user=False):
        attribs = " SUPERUSER" if super_user else ""
        self.pg_execute(f"CREATE USER {user}{attribs}")

    def pg_drop_user(self, user):
        self.pg_execute(f"DROP USER {user}")

    def pg_stop(self):
        self.ssh_run_check(f"systemctl stop {self.pg_service}")

    def pg_start(self):
        self.ssh_run_check(f"systemctl start {self.pg_service}")

    def pg_restart(self):
        self.ssh_run_check(f"systemctl restart {self.pg_service}")

    def pg_add_replication_slots(self, standbies):
        for h in standbies:
            self.pg_execute(
                f"SELECT * "
                f"FROM pg_create_physical_replication_slot('{h.pg_slot}', true)")
