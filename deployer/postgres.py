from abc import ABCMeta
from pathlib import PurePosixPath
from threading import Lock
from time import sleep

from deployer_error import DeployerError
from linux import Distro
from ssh import Ssh


class Postgres(Ssh, metaclass=ABCMeta):
    pg_version = "9.6"
    pg_lock = Lock()

    def __init__(self, *, pg_port, pg_user, pg_repl_user, **kwargs):
        super(Postgres, self).__init__(**kwargs)
        self.pg_user = pg_user
        self.pg_repl_user = pg_repl_user
        self.pg_slot = self.name.replace('-', '_')
        self.pg_port = pg_port
        self._pg_service = None
        self._pg_data_directory = None
        self._pg_hba_file = None
        self._pg_config_file = None
        self._pg_bindir = None
        self._pg_start_opts = None
        self._pg_recovery_file = None  # recovery.conf"
        self._pg_pcmk_recovery_file = None  # recovery.conf.pcmk"

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
                self.pg_datadir) / "recovery.conf"
        return self._pg_recovery_file

    @property
    def pg_pcmk_recovery_file(self):
        if self._pg_pcmk_recovery_file is None:
            self._pg_pcmk_recovery_file = PurePosixPath(
                self.pg_datadir) / "recovery.conf.pcmk"
        return self._pg_pcmk_recovery_file

    def pg_current_setting2(self, setting) -> str:
        c = f'psql -p {self.pg_port} -t -c "select current_setting(\'{setting}\');"'
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

    @property
    def pg_hba_file(self):
        if self._pg_hba_file is None:
            self._pg_hba_file = self.pg_current_setting("hba_file")
        return self._pg_hba_file

    @property
    def pg_datadir(self) -> str:
        if self._pg_data_directory is None:
            self._pg_data_directory = self.pg_current_setting("data_directory")
        return self._pg_data_directory

    @property
    def pg_config_file(self):
        if self._pg_config_file is None:
            self._pg_config_file = self.pg_current_setting("config_file")
        return self._pg_config_file

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
        pid_file = PurePosixPath(self.pg_datadir) / "postmaster.pid"
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
        self.pg_set_param("wal_level", "replica")
        self.pg_set_param("max_wal_senders", len(all_hosts) + 5)
        self.pg_set_param("max_replication_slots", len(all_hosts) + 5)
        self.pg_set_param("listen_addresses", "'*'")
        self.pg_set_param("hot_standby", "on")
        self.pg_set_param("hot_standby_feedback", "on")
        self.pg_set_param("wal_receiver_timeout", "5000")  # ms
        self.pg_set_param("wal_retrieve_retry_interval", "3000")  # ms

    def pg_write_recovery_for_pcmk(self, virtual_ip):
        repl_user = self.pg_repl_user
        self._pg_add_to_pcmk_recovery_conf("standby_mode", "on")
        self._pg_add_to_pcmk_recovery_conf("recovery_target_timeline", "latest")
        self._pg_add_to_pcmk_recovery_conf(
            "primary_conninfo",
            f"host={virtual_ip} port={self.pg_port} user={repl_user}")
        self._pg_add_to_pcmk_recovery_conf("primary_slot_name", self.pg_slot)

    def pg_standby_backup_from_master(self, master: 'Postgres'):
        """
        systemctl stop postgresql-9.6
        mv 9.6/data data_old
        pg_basebackup -h pg01 -D 9.6/data -U repl1 -v -P --xlog-method=stream
        postgresql.conf:
            hot_standby = on
        recovery.conf:
            standby_mode = on
            restore_command = 'rsync -pog pg01:wals_from_this/%f %p'
            primary_conninfo = 'host=192.168.72.101 port=5432 user=repl1'
            primary_slot_name = 'node_a'
        systemctl start postgresql-9.6
        """
        self.ssh_run_check([
            f"mv {master.pg_datadir} data_old",
            f"pg_basebackup -h {master.name} -p {self.pg_port} "
            f"-D {master.pg_datadir} -U {master.pg_repl_user} -Xs"],
            user=self.pg_user)
        self.ssh_run_check(
            f"sed -i s/{master.name}/{self.name}/ {master.pg_pcmk_recovery_file}",
            user=self.pg_user)
        self.ssh_run_check(
            f"sed -i s/{master.pg_slot}/{self.pg_slot}/ {master.pg_pcmk_recovery_file}",
            user=self.pg_user)
        self.ssh_run_check(
            f"cp {master.pg_pcmk_recovery_file} "
            f"{master.pg_recovery_file}",
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

    def pg_add_replication_slots(self, hosts):
        for h in hosts:
            self.pg_execute(
                f"SELECT * "
                f"FROM pg_create_physical_replication_slot('{h.pg_slot}')")
