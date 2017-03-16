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

    def __init__(self, *, pg_user, pg_replication_user, **kwargs):
        super(Postgres, self).__init__(**kwargs)
        self.pg_user = pg_user
        self.pg_replication_user = pg_replication_user
        self.pg_slot = f"{self.name.replace('-', '_')}_slot"
        self._pg_service = None
        self._pg_data_directory = None
        self._pg_hba_file = None
        self._pg_config_file = None
        self._pg_recovery_file = None  # recovery.conf"
        self._pg_pcmk_recovery_file = None  # recovery.conf.pcmk"

    @property
    def pg_service(self):
        if self._pg_service is None:
            distro = self.distro
            if distro == Distro.CENTOS:
                self._pg_service = f"postgresql-{Postgres.pg_version}"
            elif distro in (Distro.UBUNTU, Distro.DEBIAN):
                self._pg_service = f"postgresql"
        return self._pg_service

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
        c = f'psql -t -c "select current_setting(\'{setting}\');"'
        o, e = self.ssh_run_check(c, self.pg_user)
        return o.read().decode('utf-8').strip()

    def pg_current_setting(self, setting):
        ret = None
        tries = 0
        while not ret:
            tries += 1
            ret = self.pg_current_setting2(setting)
            if ret:
                break
            self.log(f"failed to get {setting}, will try again in one second")
            sleep(1)
            if tries > 10:
                raise DeployerError(f'waited too long after {setting} setting')
        return ret

    @property
    def pg_hba_file(self):
        if self._pg_hba_file is None:
            self._pg_hba_file = self.pg_current_setting("hba_file")
        return self._pg_hba_file

    @property
    def pg_data_directory(self) -> str:
        if self._pg_data_directory is None:
            self._pg_data_directory = self.pg_current_setting("data_directory")
        return self._pg_data_directory

    @property
    def pg_config_file(self):
        if self._pg_config_file is None:
            self._pg_config_file = self.pg_current_setting("config_file")
        return self._pg_config_file

    @property
    def pg_service(self):
        if self._pg_service is None:
            distro = self.distro
            if distro == Distro.CENTOS:
                self._pg_service = f"postgresql-{Postgres.pg_version}"
            elif distro in (Distro.UBUNTU, Distro.DEBIAN):
                self._pg_service = f"postgresql"
        return self._pg_service

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
            self.pg_user)

    def _pg_add_to_recovery_conf(self, param, val):
        self._pg_add_to_conf(param, val, self.pg_recovery_file)

    def _pg_add_to_pcmk_recovery_conf(self, param, val):
        self._pg_add_to_conf(param, val, self.pg_pcmk_recovery_file)

    def pg_execute(self, sql, user=None):
        c = f'psql -c "{sql};"'
        self.ssh_run_check(c, user if user else self.pg_user)

    def pg_create_db(self, db: str):
        self.ssh_run_check(f"createdb {db}", self.pg_user)

    def pg_drop_db(self, db: str):
        self.ssh_run(f"dropdb {db}", self.pg_user)

    def pg_create_replication_user(self):
        self.ssh_run_check(
            f'createuser {self.pg_replication_user} -c 5 --replication',
            self.pg_user)

    def pg_make_master(self, all_hosts):
        """
        pg_hba.conf:
            host replication repl1 192.168.72.102/32 trust 
        alter system set wal_level to hot_standby;
        alter system set max_wal_senders to 5;
        alter system set wal_keep_segments to 32;
        alter system set archive_mode to on;
        alter system set archive_command to 'cp %p ../../wals_from_this/%f';
        alter system set listen_addresses to '*';
        systemctl restart postgresql-9.6
        """
        cmds = [
            f'echo "host replication {h.pg_replication_user} {h.ip}/32 trust" ' 
            f'>> {self.pg_hba_file}'
            for h in all_hosts]
        self.ssh_run_check(cmds, self.pg_user)
        self.pg_set_param("wal_level", "replica")
        self.pg_set_param("max_wal_senders", "5")
        self.pg_set_param("wal_keep_segments", "32")
        self.pg_set_param("max_replication_slots", len(all_hosts))
        self.pg_set_param("archive_mode", "on")
        self.pg_set_param("archive_command", "'cp %p ../../wals_from_this/%f'")
        self.pg_set_param("listen_addresses", "'*'")
        self.pg_set_param("hot_standby", "on")
        self.pg_set_param("hot_standby_feedback", "on")

    def pg_write_recovery_for_pcmk(self):
        repl_user = self.pg_replication_user
        # self.pg_stop()
        # self.ssh_run_check([
        #     f"mv {Postgres.pg_data_dir} data_old",
        #     f"pg_basebackup -h {master.name} -D {Postgres.pg_data_dir} -U {repl_user} -Xs"],
        #     self.pg_user)
        # self._pg_add_to_conf("hot_standby", "on")
        # self._pg_add_to_conf("hot_standby_feedback ", "on")
        self._pg_add_to_pcmk_recovery_conf("standby_mode", "on")
        self._pg_add_to_pcmk_recovery_conf("recovery_target_timeline", "latest")
        # self._pg_add_to_pcmk_recovery_conf(
        #     "restore_command",
        #     f"rsync -pog {self.cluster_vip}:wals_from_this/%f %p")
        self._pg_add_to_pcmk_recovery_conf(
            "primary_conninfo",
            f"host={self.cluster_vip} port=5432 user={repl_user} "
            f"application_name={self.name}")

    def pg_standby_backup_from_master(self, master: 'Postgres'):
        """
        systemctl stop postgresql-9.6
        mv 9.6/data data_old
        pg_basebackup -h pg01 -D 9.6/data -U repl1 -v -P --xlog-method=stream
        postgresql.conf:
            hot_standby = on' 
        recovery.conf:
            standby_mode = on
            restore_command = 'rsync -pog pg01:wals_from_this/%f %p'
            primary_conninfo = 'host=192.168.72.101 port=5432 user=repl1'
            primary_slot_name = 'node_a_slot'
        systemctl start postgresql-9.6
        """
        repl_user = master.pg_replication_user
        # self.pg_stop()
        self.ssh_run_check([
            f"mv {master.pg_data_directory} data_old",
            f"pg_basebackup -h {master.name} -D {master.pg_data_directory} "
            f"-U {repl_user} -Xs"],
            self.pg_user)

        """
        # self._pg_add_to_conf("hot_standby", "on")
        # self._pg_add_to_conf("hot_standby_feedback ", "on")
        self._pg_add_to_recovery_conf("standby_mode", "on")
        self._pg_add_to_recovery_conf("recovery_target_timeline", "latest")
        self._pg_add_to_recovery_conf(
            "restore_command",
            f"rsync -pog {master.name}:wals_from_this/%f %p")
        self._pg_add_to_recovery_conf(
            "primary_conninfo",
            f"host={master.ip} port=5432 user={repl_user}")
        """
        # self._pg_add_to_pcmk_recovery_conf("primary_slot_name", self.pg_slot)
        self.ssh_run_check(
            f"sed -i s/{master.name}/{self.name}/ "
            f"{master.pg_pcmk_recovery_file}",
            self.pg_user)
        self.ssh_run_check(
            f"cp {master.pg_pcmk_recovery_file} "
            f"{master.pg_recovery_file}",
            self.pg_user)

    def pg_create_user(self, user, super_user=False):
        attribs = " SUPERUSER" if super_user else ""
        self.pg_execute(f"CREATE USER {user}{attribs}")

    def pg_drop_user(self, user):
        self.pg_execute(f"DROP USER {user}")

    def pg_create_wal_dir(self):
        self.ssh_run_check('mkdir -p wals_from_this', self.pg_user)

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
