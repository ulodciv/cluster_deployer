#!/usr/bin/python2.7
from __future__ import division, print_function, unicode_literals
import json
import os
import pwd
import re
import sys
import tempfile
from collections import defaultdict, OrderedDict
from datetime import datetime
from distutils.version import LooseVersion
from functools import partial
from itertools import chain
from subprocess import call, check_output, CalledProcessError, STDOUT
from tempfile import gettempdir
from time import sleep

VERSION = "1.0"
PROGRAM = "pgha"
MIN_PG_VER = LooseVersion('9.6')
OCF_SUCCESS = 0
OCF_ERR_GENERIC = 1
OCF_ERR_ARGS = 2
OCF_ERR_UNIMPLEMENTED = 3
OCF_ERR_PERM = 4
OCF_ERR_INSTALLED = 5
OCF_ERR_CONFIGURED = 6
OCF_NOT_RUNNING = 7
OCF_RUNNING_MASTER = 8
OCF_FAILED_MASTER = 9
RE_TPL_SLOT = r"^\s*primary_slot_name\s*=\s*'?(.*?)'?\s*$"
RE_TPL_TIMELINE = r"^\s*recovery_target_timeline\s*=\s*'?latest'?\s*$"
RE_TPL_STANDBY_MODE = r"^\s*standby_mode\s*=\s*'?on'?\s*$"
RE_PG_CLUSTER_STATE = r"^Database cluster state:\s+(.*?)\s*$"
pguser_default = "postgres"
pgbindir_default = "/usr/bin"
pgdata_default = "/var/lib/pgsql/data"
pghost_default = "/var/run/postgresql"
pgport_default = "5432"
OCF_META_DATA = """\
<?xml version="1.0"?>
<!DOCTYPE resource-agent SYSTEM "ra-api-1.dtd">
<resource-agent name="pgsqlha">
<version>1.0</version>
<longdesc lang="en">Multi-state PostgreSQL resource agent. It manages 
PostgreSQL instances using streaming replication.</longdesc>
<shortdesc lang="en">Manages replicated PostgreSQL instances</shortdesc>
<parameters>
    <parameter name="pguser" unique="0" required="0">
    <longdesc lang="en">PostgreSQL server user</longdesc>
    <shortdesc lang="en">PostgreSQL server user</shortdesc>
    <content type="string" default="{pguser_default}" />
</parameter>
<parameter name="pgbindir" unique="0" required="0">
    <longdesc lang="en">Directory with PostgreSQL binaries. This RA uses 
    psql, pg_isready, pg_controldata and pg_ctl.</longdesc>
    <shortdesc lang="en">Path to PostgreSQL binaries</shortdesc>
    <content type="string" default="{pgbindir_default}" />
</parameter>
<parameter name="pgdata" unique="1" required="0">
    <longdesc lang="en">Data directory</longdesc>
    <shortdesc lang="en">pgdata</shortdesc>
    <content type="string" default="{pgdata_default}" />
</parameter>
<parameter name="pghost" unique="0" required="0">
    <longdesc lang="en">Host IP address or unix socket folder.</longdesc>
    <shortdesc lang="en">pghost</shortdesc>
    <content type="string" default="{pghost_default}" />
</parameter>
<parameter name="pgport" unique="0" required="0">
    <longdesc lang="en">PostgreSQL port.</longdesc>
    <shortdesc lang="en">pgport</shortdesc>
    <content type="integer" default="{pgport_default}" />
</parameter>
<parameter name="pgconf" unique="0" required="0">
    <longdesc lang="en">
    Additionnal arguments for 'pg_ctl start' for the -o parameter. 
    Can be use when postgresql.conf file is not in PGDATA, eg.:
    "-c config_file=/etc/postgresql/9.6/main/postgresql.conf".</longdesc>
    <shortdesc lang="en">Additionnal arguments for 'pg_ctl start'.</shortdesc>
    <content type="string" default="{pgdata_default}/postgresql.conf" />
</parameter>
</parameters>
<actions>
    <action name="start" timeout="60" />
    <action name="stop" timeout="60" />
    <action name="promote" timeout="30" />
    <action name="demote" timeout="120" />
    <action name="monitor" depth="0" timeout="10" interval="15"/>
    <action name="monitor" depth="0" timeout="10" interval="15" role="Master"/>
    <action name="monitor" depth="0" timeout="10" interval="16" role="Slave"/>
    <action name="notify" timeout="60" />
    <action name="meta-data" timeout="5" />
    <action name="validate-all" timeout="5" />
    <action name="methods" timeout="5" />
</actions>
</resource-agent>""".format(**globals())
OCF_METHODS = """\
start
stop
promote
demote
monitor
notify
methods
meta-data
validate-all"""
ACTION = sys.argv[1] if len(sys.argv) > 1 else None
_logtag = None


def hadate():
    return datetime.now().strftime(os.environ.get('HA_DATEFMT', '%Y/%m/%d_%T'))


def get_pguser():
    return os.environ.get('OCF_RESKEY_pguser', pguser_default)


def get_pgbindir():
    return os.environ.get('OCF_RESKEY_pgbindir', pgbindir_default)


def get_pgdata():
    return os.environ.get('OCF_RESKEY_pgdata', pgdata_default)


def get_pghost():
    return os.environ.get('OCF_RESKEY_pghost', pghost_default)


def get_pgport():
    return os.environ.get('OCF_RESKEY_pgport', pgport_default)


def get_pgconf():
    return os.environ.get(
        "OCF_RESKEY_pgconf", os.path.join(get_pgdata(), "postgresql.conf"))


def get_recovery_pcmk():
    return os.path.join(get_pgdata(), "recovery.conf.pcmk")


def get_pgctl():
    return os.path.join(get_pgbindir(), "pg_ctl")


def get_psql():
    return os.path.join(get_pgbindir(), "psql")


def get_pgctrldata():
    return os.path.join(get_pgbindir(), "pg_controldata")


def get_pgisready():
    return os.path.join(get_pgbindir(), "pg_isready")


def get_ha_bin():
    return os.environ.get('HA_SBIN_DIR', '/usr/sbin')


def get_ha_debuglog():
    return os.environ.get('HA_DEBUGLOG', "")


def get_crm_master():
    return os.path.join(get_ha_bin(), "crm_master") + " --lifetime forever"


def get_crm_node():
    return os.path.join(get_ha_bin(), "crm_node")


def get_logtag():
    global _logtag
    if not _logtag:
        _logtag = "{}:{}({})[{}]".format(
            PROGRAM, ACTION, os.environ['OCF_RESOURCE_INSTANCE'], os.getpid())
    return _logtag


def ocf_log(level, msg, *args):
    LOG_TAG = get_logtag()
    l = level + ": " + msg.format(*args)
    if os.environ.get('HA_LOGFACILITY', 'none') == 'none':
        os.environ['HA_LOGFACILITY'] = ""
    ha_logfacility = os.environ.get('HA_LOGFACILITY', "")
    # if we're connected to a tty, then output to stderr
    if sys.stderr.isatty():
        sys.stderr.write("{}: {}\n".format(LOG_TAG, l))
        return 0
    if os.environ.get('HA_LOGD', "") == 'yes':
        if call(['ha_logger', '-t', LOG_TAG, l]) == 0:
            return 0
    if ha_logfacility != "":
        # logging through syslog
        # loglevel is unknown, use 'notice' for now
        if level in ("ERROR", "CRIT"):
            level = "err"
        elif level == "WARNING":
            level = "warning"
        elif level == "INFO":
            level = "info"
        elif level == "DEBUG":
            level = "debug"
        else:
            level = "notice"
        call(["logger", "-t", LOG_TAG, "-p", ha_logfacility + "." + level, l])
    ha_logfile = os.environ.get("HA_LOGFILE")
    if ha_logfile:
        with open(ha_logfile, "a") as f:
            f.write("{}: {} {}\n".format(LOG_TAG, hadate(), l))
    elif not ha_logfacility:
        sys.stderr.write("{} {}\n".format(hadate(), l))
    ha_debuglog = get_ha_debuglog()
    if ha_debuglog and ha_debuglog != ha_logfile:
        with open(ha_debuglog, "a") as f:
            f.write("{}: {} {}\n".format(LOG_TAG, hadate(), l))


log_crit = partial(ocf_log, "CRIT")
log_err = partial(ocf_log, "ERROR")
log_warn = partial(ocf_log, "WARNING")
log_info = partial(ocf_log, "INFO")
log_debug = partial(ocf_log, "DEBUG")


def get_attrd_updater():
    return os.path.join(get_ha_bin(), "attrd_updater")


def get_ha_private_attr(name, node=None):
    cmd = [get_attrd_updater(), "-Q", "-n", name, "-p"]
    if node:
        cmd.extend(["-N", node])
    try:
        ans = check_output(cmd)
    except CalledProcessError:
        return ""
    p = re.compile(r'^name=".*" host=".*" value="(.*)"$')
    m = p.findall(ans)
    if not m:
        return ""
    return m[0]


def set_ha_private_attr(name, val):
    return call(
        [get_attrd_updater(), "-U", val, "-n", name, "-p", "-d", "0"]) == 0


def del_ha_private_attr(name):
    return call([get_attrd_updater(), "-D", "-n", name, "-p", "-d", "0"]) == 0


def run_pgctrldata():
    try:
        return check_output([get_pgctrldata(), get_pgdata()])
    except CalledProcessError as e:
        return e.output


def get_ocf_nodename():
    try:
        return check_output([get_crm_node(), "-n"]).strip()
    except CalledProcessError:
        sys.exit(OCF_ERR_GENERIC)


def run_pgisready():
    return as_postgres([get_pgisready(), "-h", get_pghost(), "-p", get_pgport()])


def as_postgres_user():
    u = pwd.getpwnam(get_pguser())
    os.initgroups(get_pguser(), u.pw_gid)
    os.setgid(u.pw_gid)
    os.setuid(u.pw_uid)
    os.seteuid(u.pw_uid)


def as_postgres(cmd):
    cmd = [str(c) for c in cmd]
    log_debug("as {}: {}", get_pguser(), " ".join(cmd))
    with open(os.devnull, "w") as DEVNULL:
        return call(
            cmd, preexec_fn=as_postgres_user, stdout=DEVNULL, stderr=STDOUT)


def pg_execute(query):
    RS = chr(30)  # record separator
    FS = chr(3)  # end of text
    try:
        tmp_fh, tmp_file = tempfile.mkstemp(prefix="pgsqlms-")
        os.write(tmp_fh, query)
        os.close(tmp_fh)
        os.chmod(tmp_file, 0o644)
    except:
        log_crit("could not create or write in a temp file")
        sys.exit(OCF_ERR_INSTALLED)
    try:
        cmd = [
            get_psql(), "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-qXAtf",
            tmp_file, "-R", RS, "-F", FS, "-p", get_pgport(), "-h", get_pghost()]
        log_debug(" ".join(cmd).replace(RS, "<RS>").replace(FS, "<FS>"))
        ans = check_output(cmd, preexec_fn=as_postgres_user)
    except CalledProcessError as e:
        log_debug("psql error, return code: {}", e.returncode)
        # Possible return codes:
        #  -1: wrong parameters
        #   1: failed to get resources (memory, missing file, ...)
        #   2: unable to connect
        #   3: query failed
        return e.returncode, []
    finally:
        os.remove(tmp_file)
    log_debug("executed query:\n{}", query)
    rs = []
    if ans:
        ans = ans[:-1]
        for record in ans.split(RS):
            rs.append(record.split(FS))
        log_debug("rs: {}", rs)
    return 0, rs


def get_ha_nodes():
    try:
        return check_output([get_crm_node(), "-p"]).split()
    except CalledProcessError as e:
        log_err("{} failed with return code {}", e.cmd, e.returncode)
        sys.exit(OCF_ERR_GENERIC)


def set_standbies_scores():
    """ Set scores to better the odds that the standby closest to the master
    will be the selected when Pacemaker chooses a standby to promote. The idea
    is to avoid going through pre-promote and finding out that there is a better
    standby. The promotion would then be canceled, and Pacemaker would promote
    that other standby
    Notes regarding scores:
    - Highest is best
    - Master has score 1001
    - Standbies get 999, 998, ...
    - Bad nodes get -1
    - Unknown nodes get -1000 """
    nodename = get_ocf_nodename()
    nodes_to_score = get_ha_nodes()
    rc, rs = pg_execute(
        "SELECT slot_name, restart_lsn, active_pid FROM pg_replication_slots "
        "WHERE active AND slot_type='physical' ORDER BY restart_lsn")
    if rc != 0:
        log_err("failed to get replication slots info")
        sys.exit(OCF_ERR_GENERIC)
    # For each standby connected, set their master score based on the following
    # rule: the first known node, with the highest priority and
    # with an acceptable state.
    for i, (slot_name, restart_lsn, active_pid) in enumerate(rs):
        for node in nodes_to_score:
            if node.replace('-', '_') == slot_name:
                node_to_score = node
                break
        else:
            log_info("ignoring unknown physical slot {}", slot_name)
            continue
        rc, rs = pg_execute(
            "SELECT state FROM pg_stat_replication WHERE pid={}".format(
                active_pid))
        if rc != 0:
            log_err("failed to get replication slots info")
            sys.exit(OCF_ERR_GENERIC)
        state = rs[0][0].strip()
        if state in ("startup", "backup"):
            # We exclude any standby being in state backup (pg_basebackup) or
            # startup (new standby or failing standby)
            log_info("forbid promotion of {} because it has state '{}': "
                     "set its score to -1", node_to_score, state)
            set_promotion_score("-1", node_to_score)
        else:
            score = 1000 - i - 1
            log_info("update score of {} to {}", node_to_score, score)
            set_promotion_score(score, node_to_score)
        # Remove this node from the known nodes list.
        nodes_to_score.remove(node_to_score)
    # If there are still nodes in "nodes_to_score", it means there is no
    # corresponding line in "pg_stat_replication".
    for node in nodes_to_score:
        if node == nodename:  # Exclude this node
            continue
        log_warn("{} is not connected to the master, set score to -1000", node)
        set_promotion_score("-1000", node)
    set_promotion_score("1001")
    return OCF_SUCCESS


def is_master_or_standby():
    """ Confirm if the instance is really started as pg_isready stated and
    if the instance is master or standby
    Return OCF_SUCCESS or OCF_RUNNING_MASTER or OCF_ERR_GENERIC or
    OCF_ERR_CONFIGURED """
    rc, rs = pg_execute("SELECT pg_is_in_recovery()")
    if rc == 0:
        is_in_recovery = rs[0][0]
        if is_in_recovery == "t":
            log_debug("PG is a running as s standby")
            return OCF_SUCCESS
        else:  # is_in_recovery == "f":
            log_debug("PG is running as a master")
            return OCF_RUNNING_MASTER
    elif rc in (1, 2):
        # psql cound not connect; pg_isready reported PG is listening, so this
        # error could be a max_connection saturation: report a soft error
        log_err("psql can't connect to server")
        return OCF_ERR_GENERIC
    # The query failed (rc: 3) or bad parameters (rc: -1).
    # This should not happen, raise a hard configuration error.
    log_err("query to check if PG is a master or standby failed, rc: {}", rc)
    return OCF_ERR_CONFIGURED


def get_pgctrldata_state():
    """ Parse and return the current status of the local PostgreSQL instance as
    reported by its controldata file
    WARNING: the status is NOT updated in case of crash. """
    datadir = get_pgdata()
    finds = re.findall(RE_PG_CLUSTER_STATE, run_pgctrldata(), re.M)
    if not finds:
        log_crit("couldn't read state from controldata file for {}", datadir)
        sys.exit(OCF_ERR_CONFIGURED)
    log_debug("PG state is '{}'", finds[0])
    return finds[0]


def get_non_transitional_pg_state():
    """ Loop until pg_controldata returns a non-transitional state.
    Used to find out if this instance is a master or standby.
    Return OCF_RUNNING_MASTER or OCF_SUCCESS or OCF_NOT_RUNNING  """
    while True:
        state = get_pgctrldata_state()
        if state == "":
            # Something went really wrong with pg_controldata.
            log_err("empty PG cluster state")
            sys.exit(OCF_ERR_INSTALLED)
        if state == "in production":
            return OCF_RUNNING_MASTER
        # Hot or warm (rejects connections, including from pg_isready) standby
        if state == "in archive recovery":
            return OCF_SUCCESS
        # stopped
        if state in ("shut down",  # was a Master
                     "shut down in recovery"  # was a standby
                     ):
            return OCF_NOT_RUNNING
        # The state is "in crash recovery", "starting up" or "shutting down".
        # This state should be transitional, so we wait and loop to check if
        # it changes.
        # If it does not, pacemaker will eventually abort with a timeout.
        log_debug("waiting for transitionnal state '{}' to change", state)
        sleep(1)


def pg_ctl_status():
    """ checks whether PG is running
    Return 0 if the server is running, 3 otherwise """
    return as_postgres([get_pgctl(), "status", "-D", get_pgdata()])


def pg_ctl_start():
    # long timeout to ensure Pacemaker gives up first
    return as_postgres([get_pgctl(), "start", "-D", get_pgdata(), "-w",
                        "-t", 1000000, "-o", "-c config_file=" + get_pgconf()])


def pg_ctl_stop():
    # long timeout to ensure Pacemaker gives up first
    return as_postgres([get_pgctl(), "stop", "-D", get_pgdata(),
                        "-w", "-t", 1000000, "-m", "fast"])


def backup_label_exists():
    return os.path.isfile(os.path.join(get_pgdata(), "backup_label"))


def confirm_stopped():
    """ PG is really stopped and it was propertly shut down """
    # Check the postmaster process status.
    pgctlstatus_rc = pg_ctl_status()
    if pgctlstatus_rc == 0:
        # The PID file exists and the process is available.
        # That should not be the case, return an error.
        log_err("PG is not listening, but the process in postmaster.pid exists")
        return OCF_ERR_GENERIC
    # The PID file does not exist or the process is not available.
    log_debug("no PG server process found")
    if backup_label_exists():
        log_debug("found backup_label: indicates a never started backup")
        return OCF_NOT_RUNNING
    # Continue the check with pg_controldata.
    controldata_rc = get_non_transitional_pg_state()
    if controldata_rc == OCF_RUNNING_MASTER:
        # The controldata has not been updated to "shutdown".
        # It should mean we had a crash on a master instance.
        log_err("PG's controldata indicates a running "
                "master, the instance has probably crashed")
        return OCF_FAILED_MASTER
    elif controldata_rc == OCF_SUCCESS:
        # The controldata has not been updated to "shutdown in recovery".
        # It should mean we had a crash on a standby instance.
        # There is no "FAILED_SLAVE" return code, so we return a generic error.
        log_warn(
            "PG appears to be a crashed standby, let's pretend all is good "
            "so that Pacemaker tries to start it back as-is")
        return OCF_NOT_RUNNING
    elif controldata_rc == OCF_NOT_RUNNING:
        # The controldata state is consistent, the instance was probably
        # propertly shut down.
        log_debug("PG's controldata indicates "
                  "that the instance was propertly shut down")
        return OCF_NOT_RUNNING
    # Something went wrong with the controldata check.
    log_err("couldn't get PG's status from controldata (returned {})",
            controldata_rc)
    return OCF_ERR_GENERIC


def create_recovery_conf():
    """ Write recovery.conf. It must include:
    standby_mode = on
    primary_conninfo = 'host=<VIP> port=5432 user=repl1'
    recovery_target_timeline = latest
    primary_slot_name = <node_name> """
    u = pwd.getpwnam(get_pguser())
    uid, gid = u.pw_uid, u.pw_gid
    recovery_file = os.path.join(get_pgdata(), "recovery.conf")
    recovery_tpl = get_recovery_pcmk()
    log_debug("get replication configuration from template {}", recovery_tpl)
    # primary_conninfo often has a virtual IP to reach the master. There is no
    # master available at startup, so standbies will complain about failing to
    # connect. This is normal.
    try:
        with open(recovery_tpl) as fh:
            # Copy all parameters from the template file
            recovery_conf = fh.read()
    except:
        log_crit("can't open {}", recovery_tpl)
        sys.exit(OCF_ERR_CONFIGURED)
    log_debug("writing {}", recovery_file)
    try:
        # Write recovery.conf using configuration from the template file
        with open(recovery_file, "w") as fh:
            fh.write(recovery_conf)
    except:
        log_crit("can't open {}", recovery_file)
        sys.exit(OCF_ERR_CONFIGURED)
    try:
        os.chown(recovery_file, uid, gid)
    except:
        log_crit("can't set owner of {}", recovery_file)
        sys.exit(OCF_ERR_CONFIGURED)


def get_promotion_score(node=None):
    cmd = [get_crm_master(), "--quiet", "--get-value"]
    if node:
        cmd.extend(["-N", node])
    try:
        score = check_output(" ".join(cmd), shell=True)
    except CalledProcessError:
        return ""
    return score.strip()


def no_node_has_a_positive_score():
    for node in get_ha_nodes():
        try:
            if int(get_promotion_score(node)) >= 0:
                return False
        except ValueError:
            pass
    return True


def set_promotion_score(score, node=None):
    score = str(score)
    cmd = [get_crm_master(), "-q", "-v", score]
    if node:
        cmd.extend(["-N", node])
    call(" ".join(cmd), shell=True)
    # setting attributes is asynchronous, so return as soon as truly done
    while True:
        tmp = get_promotion_score(node)
        if tmp == score:
            break
        log_debug("waiting to set score to {} (currently {})...", score, tmp)
        sleep(0.1)


def ocf_promote():
    state = is_master_or_standby()
    if state == OCF_RUNNING_MASTER:
        # Already a master. Unexpected, but not a problem.
        log_warn("PG already running as a master")
        return OCF_SUCCESS
    elif state != OCF_SUCCESS:
        log_err("Unexpected error, cannot promote this node")
        return OCF_ERR_GENERIC
    # Running as standby. Normal, expected behavior.
    log_debug("PG running as a standby")
    # Cancel if it has been considered unsafe during pre-promote
    if get_ha_private_attr("cancel_promotion") == "1":
        log_err("Promotion was cancelled during pre-promote")
        del_ha_private_attr("cancel_promotion")
        return OCF_ERR_GENERIC
    # Do not check for a better candidate if we try to recover the master
    # Recovery of a master is detected during pre-promote. It is signaled by
    # setting the 'master_promotion' flag.
    if get_ha_private_attr("master_promotion") == "1":
        log_info("promoting the previous master, no need to make sure this node"
                 "is the most up to date")
    else:
        # The best standby to promote has the highest LSN. If this node
        # is not the best one, we need to modify the master scores accordingly,
        # and abort the current promotion
        if not confirm_this_node_should_be_promoted():
            return OCF_ERR_GENERIC
    # replication slots can be added on hot standbies, helps ensuring no WAL is
    # missed after promotion
    if not add_replication_slots(get_ha_nodes()):
        log_err("failed to add all replication slots")
        return OCF_ERR_GENERIC
    if as_postgres([get_pgctl(), "promote", "-D", get_pgdata()]) != 0:
        # Promote the instance on the current node.
        log_err("'pg_ctl promote' failed")
        return OCF_ERR_GENERIC
    # promote is asynchronous: wait for it to finish, at least with PG 9.6
    while is_master_or_standby() != OCF_RUNNING_MASTER:
        log_debug("waiting for promote to complete")
        sleep(1)
    log_info("promote complete")
    return OCF_SUCCESS


def add_replication_slots(slave_nodes):
    nodename = get_ocf_nodename()
    rc, rs = pg_execute("SELECT slot_name FROM pg_replication_slots")
    slots = [r[0] for r in rs]
    for node in slave_nodes:
        if node == nodename:
            continue  # TODO: is this check necessary?
        slot = node.replace('-', '_')
        if slot in slots:
            continue
        rc, rs = pg_execute(
            "SELECT * FROM pg_create_physical_replication_slot('{}', true)".format(
                slot))
        if rc != 0:
            log_err("failed to add replication slot {}".format(slot))
            return False
        log_debug("added replication slot {}".format(slot))
    return True


def kill_wal_sender(slot):
    rc, rs = pg_execute(
        "SELECT pg_terminate_backend(active_pid) "
        "FROM pg_replication_slots "
        "WHERE slot_name='{}' AND active_pid IS NOT NULL".format(slot))
    if rs:
        log_info("killed active wal sender for slot {}".format(slot))


def kill_wal_senders(slave_nodes):
    nodename = get_ocf_nodename()
    rc, rs = pg_execute("SELECT slot_name "
                        "FROM pg_replication_slots "
                        "WHERE active_pid IS NOT NULL")
    slots = [r[0] for r in rs]
    for node in slave_nodes:
        if node == nodename:
            continue  # TODO: is this check necessary?
        slot = node.replace('-', '_')
        if slot in slots:
            kill_wal_sender(slot)


def delete_replication_slots():
    rc, rs = pg_execute("SELECT slot_name FROM pg_replication_slots")
    if not rs:
        log_debug("no replication slots to delete")
        return True
    for slot in [r[0] for r in rs]:
        kill_wal_sender(slot)
        q = "SELECT * FROM pg_drop_replication_slot('{}')".format(slot)
        rc, rs = pg_execute(q)
        if rc != 0:
            log_err("failed to delete replication slot {}".format(slot))
            return False
        log_debug("deleted replication slot {}".format(slot))
    return True


def confirm_this_node_should_be_promoted():
    """ Find out if this node is truly the one to promote. If not, also update
    the scores.
    The standby with the highest master score gets promoted, as set during the
    last monitor on the previous master (see set_standbies_scores). Confirm
    that these scores are still correct in that this node still has the most
    advance WAL record. All slaves should all have set the "replay_location"
    resource attribute during pre-promote.
    Return True if this node has the highest replay_location LSN, False
    otherwise """
    log_debug("checking if current node is the best for promotion")
    # Exclude nodes that are known to be unavailable (not in the current
    # partition) using the "crm_node" command
    active_nodes = get_ha_private_attr("nodes").split()
    local_node = get_ocf_nodename()
    node_to_promote = local_node
    local_lsn = get_ha_private_attr("replay_location")  # set during pre-promote
    if local_lsn == "":
        log_crit("no replay location LSN for this node")
        return False
    # convert LSN to a pair of integers
    local_lsn = [int(v, 16) for v in local_lsn.split("/")]
    highest_lsn = local_lsn
    log_debug("replay location LSN for this node: {}", local_lsn)
    # Now we compare with the other available nodes.
    for node in (n for n in active_nodes if n != local_node):
        node_lsn = get_ha_private_attr("replay_location", node)  # set during pre-promote
        if node_lsn == "":
            log_crit("no replay location LSN for {}", node)
            return False
        # convert location to decimal
        node_lsn = [int(v, 16) for v in node_lsn.split("/")]
        log_debug("replay location LSN for {}: {}", node, node_lsn)
        if node_lsn > highest_lsn:
            node_to_promote = node
            highest_lsn = node_lsn
            log_debug("{}'s replay location is higher", node)
    # If any node has been selected, we adapt the master scores accordingly
    # and break the current promotion.
    if node_to_promote != local_node:
        log_info("{} is the best standby to promote, aborting promotion",
                 node_to_promote)
        set_promotion_score("1")  # minimize local node's score
        set_promotion_score("1000", node_to_promote)
        # Make promotion fail; another one will occur with updated scores
        return False
    return True


def del_private_attributes():
    del_ha_private_attr("replay_location")
    del_ha_private_attr("master_promotion")
    del_ha_private_attr("nodes")
    del_ha_private_attr("cancel_promotion")


def notify_pre_promote(nodes):
    node = get_ocf_nodename()
    promoting = nodes["promote"][0]
    log_info("{} will be promoted", promoting)

    # No need to do an election between slaves if this is recovery of the master
    if promoting in nodes["master"]:
        log_warn("this is a master recovery")
        if promoting == node:
            set_ha_private_attr("master_promotion", "1")
        return OCF_SUCCESS

    del_private_attributes()

    # If the sub keeps going, that means the switchover is safe.
    # Keep going with the election process in case the switchover was
    # instruct to the wrong node.
    # FIXME: should we allow a switchover to a lagging standby?

    # We need to trigger an election between existing slaves to promote the best
    # one based on its current LSN location. The designated standby for
    # promotion is responsible to connect to each available nodes to check their
    # "replay_location".
    # ocf_promote will use this information to check if the instance to promote
    # is the best one, so we can avoid a race condition between the last
    # successful monitor on the previous master and the current promotion.
    rc, rs = pg_execute("SELECT pg_last_xlog_replay_location()")
    if rc != 0:
        log_warn("could not get replay location LSN")
        # Return codes are ignored during notifications...
        return OCF_SUCCESS
    node_lsn = rs[0][0]
    log_info("replay location LSN: {}", node_lsn)
    # Set the "replay_location" attribute value for this node so we can use it
    # during the following "promote" action.
    if not set_ha_private_attr("replay_location", node_lsn):
        log_warn("could not set the replay location LSN")
    # If this node is the future master, keep track of the slaves that received
    # the same notification to compare our LSN with them during promotion
    active_nodes = defaultdict(int)
    if promoting == node:
        # build the list of active nodes:  master + slave + start - stop
        for uname in chain(nodes["master"], nodes["slave"], nodes["start"]):
            active_nodes[uname] += 1
        for uname in nodes["stop"]:
            active_nodes[uname] -= 1
        attr_nodes = " ".join(k for k in active_nodes if active_nodes[k] > 0)
        set_ha_private_attr("nodes", attr_nodes)
    return OCF_SUCCESS


def notify_pre_demote(nodes):
    # do nothing if the local node will not be demoted
    node = get_ocf_nodename()
    if node not in nodes["demote"]:
        return OCF_SUCCESS
    rc = get_ocf_state()
    # do nothing if this is not a master recovery
    master_recovery = node in nodes["master"] and node in nodes["promote"]
    if not master_recovery or rc != OCF_FAILED_MASTER:
        return OCF_SUCCESS
    # in case of master crash, we need to detect if the CRM tries to recover
    # the master clone. The usual transition is to do:
    #   demote->stop->start->promote
    #
    # There are multiple flaws with this transition:
    #  * the 1st and 2nd actions will fail because the instance is in
    #    OCF_FAILED_MASTER step
    #  * the usual start action is dangerous as the instance will start with
    #    a recovery.conf instead of entering a normal recovery process
    #
    # To avoid this, we try to start the instance in recovery from here.
    # If it success, at least it will be demoted correctly with a normal
    # status. If it fails, it will be catched up in next steps.
    log_info("trying to start PG failing master")
    # Either the instance managed to start or it couldn't.
    # We rely on the pg_ctk '-w' switch to take care of this. If it couldn't
    # start, this error will be catched up later during the various checks
    pg_ctl_start()
    log_info("state is '{}' after recovery attempt",
             get_pgctrldata_state())
    return OCF_SUCCESS


def notify_pre_start(nodes):
    """ If this is a Master:
        - add any missing replication slots
        - kill any active orphaned wal sender
    Return OCF_SUCCESS (sole possibility AFAICT) """
    if get_ocf_nodename() in nodes["master"]:
        add_replication_slots(nodes["start"])
        kill_wal_senders(nodes["start"])
    return OCF_SUCCESS


def notify_pre_stop(nodes):
    node = get_ocf_nodename()
    # do nothing if the local node will not be stopped
    if node not in nodes["stop"]:
        return OCF_SUCCESS
    rc = get_non_transitional_pg_state()
    # do nothing if this is not a standby recovery
    standby_recovery = node in nodes["slave"] and node in nodes["start"]
    if not standby_recovery or rc != OCF_SUCCESS:
        return OCF_SUCCESS
    # in case of standby crash, we need to detect if the CRM tries to recover
    # the slaveclone. The usual transition is to do: stop->start
    #
    # This transition can not work because the instance is in
    # OCF_ERR_GENERIC step. So the stop action will fail, leading most
    # probably to fencing action.
    #
    # To avoid this, we try to start the instance in recovery from here.
    # If it succeeds, at least it will be stopped correctly with a normal
    # status. If it fails, it will be catched up in next steps.
    log_info("starting standby that was not shutdown cleanly")
    # Either the instance managed to start or it couldn't.
    # We rely on the pg_ctl '-w' switch to take care of this. If it couldn't
    # start, this error will be catched up later during the various checks
    pg_ctl_start()
    log_info("state is '{}' after recovery attempt",
             get_pgctrldata_state())
    return OCF_SUCCESS


def ocf_validate_all():
    for prog in [get_pgctl(), get_psql(), get_pgisready(), get_pgctrldata()]:
        if not os.access(prog, os.X_OK):
            log_crit("{} is missing or not executable", prog)
            sys.exit(OCF_ERR_INSTALLED)
    datadir = get_pgdata()
    if not os.path.isdir(datadir):
        log_err("PGDATA {} not found".format(datadir))
        sys.exit(OCF_ERR_ARGS)
    # check PG_VERSION
    pgver_file = os.path.join(datadir, "PG_VERSION")
    try:
        with open(pgver_file) as fh:
            ver = fh.read()
    except Exception as e:
        log_crit("Can't read PG version file: {}", pgver_file, e)
        sys.exit(OCF_ERR_ARGS)
    try:
        ver = LooseVersion(ver)
    except:
        log_crit("Can't parse PG version: {}", ver)
        sys.exit(OCF_ERR_ARGS)
    if ver < MIN_PG_VER:
        log_err("PostgreSQL {} is too old: >= {} required", ver, MIN_PG_VER)
        sys.exit(OCF_ERR_INSTALLED)
    # check recovery template
    recovery_tpl = get_recovery_pcmk()
    if not os.path.isfile(recovery_tpl):
        log_crit("Recovery template {} not found".format(recovery_tpl))
        sys.exit(OCF_ERR_ARGS)
    try:
        with open(recovery_tpl) as f:
            content = f.read()
    except:
        log_crit("Could not open or read file {}".format(recovery_tpl))
        sys.exit(OCF_ERR_ARGS)
    if not re.search(RE_TPL_STANDBY_MODE, content, re.M):
        log_crit("standby_mode not 'on' in {}", recovery_tpl)
        sys.exit(OCF_ERR_ARGS)
    if not re.search(RE_TPL_TIMELINE, content, re.M):
        log_crit("recovery_target_timeline not 'latest' in {}", recovery_tpl)
        sys.exit(OCF_ERR_ARGS)
    finds = re.findall(RE_TPL_SLOT, content, re.M)
    if not finds:
        log_crit("primary_slot_name missing from {}", recovery_tpl)
        sys.exit(OCF_ERR_ARGS)
    expected_slot_name = get_ocf_nodename().replace("-", "_")
    if not finds[0] == expected_slot_name:
        log_crit("unexpected primary_slot_name in {}; expected {}, found {}",
                 recovery_tpl, expected_slot_name, finds[0])
        sys.exit(OCF_ERR_ARGS)
    try:
        pwd.getpwnam(get_pguser())
    except KeyError:
        log_crit("System user {} does not exist", get_pguser())
        sys.exit(OCF_ERR_ARGS)
    # require wal_level >= hot_standby
    # rc, rs = pg_execute("SHOW wal_level")
    # if rc != 0:
    #     log_crit("Could not get wal_level setting")
    #     sys.exit(OCF_ERR_ARGS)
    # if rs[0][0] not in ("logical", "replica"):
    #     log_crit("wal_level must be replica, or higher (logical); "
    #              "currently set to {}", rs[0][0])
    #     sys.exit(OCF_ERR_ARGS)
    return OCF_SUCCESS


def ocf_start():
    """ Start as a standby """
    rc = get_ocf_state()
    # Instance must be stopped or running as standby
    if rc == OCF_SUCCESS:
        log_info("PG is already started")
        return OCF_SUCCESS
    elif rc != OCF_NOT_RUNNING:
        log_err("unexpected PG state: {}", rc)
        return OCF_ERR_GENERIC
    return start_pg()


def start_pg():
    # From here, the instance is NOT running
    prev_state = get_pgctrldata_state()
    log_debug("starting PG as a standby")
    create_recovery_conf()
    rc = pg_ctl_start()
    if rc != 0:
        log_err("failed to start PG, rc: {}", rc)
        return OCF_ERR_GENERIC
    while True:  # Wait for the start to finish.
        rc = get_ocf_state()
        if rc != OCF_NOT_RUNNING:
            break
        sleep(1)
    if rc != OCF_SUCCESS:
        log_err("PG is not running as a standby (returned {})", rc)
        return OCF_ERR_GENERIC
    log_info("PG started")
    if not delete_replication_slots():
        log_err("failed to delete all replication slots")
        return OCF_ERR_GENERIC
    # On first cluster start, no score exists on any standbies unless manually
    # set with crm_master. Without scores, the cluster won't promote any slave.
    # Fix that by setting a score of 1, but only if this node was a master
    # before, hence picking the node that was used as master during setup.
    if prev_state == "shut down" and no_node_has_a_positive_score():
        log_info("no master score around; set mine to 1")
        set_promotion_score("1")
    return OCF_SUCCESS


def ocf_stop():
    """ Return OCF_SUCCESS or OCF_ERR_GENERIC """
    rc = get_ocf_state()
    if rc == OCF_NOT_RUNNING:
        log_info("PG already stopped")
        return OCF_SUCCESS
    elif rc not in (OCF_SUCCESS, OCF_RUNNING_MASTER):
        log_warn("unexpected PG state: {}", rc)
        return OCF_ERR_GENERIC
    # From here, the instance is running for sure.
    log_debug("PG is running, stopping it")
    # Try to quit with proper shutdown.
    # insanely long timeout to ensure Pacemaker gives up first
    if pg_ctl_stop() == 0:
        log_info("PG stopped")
        return OCF_SUCCESS
    log_err("PG failed to stop")
    return OCF_ERR_GENERIC


def loop_until_pgisready():
    while True:
        rc = run_pgisready()
        if rc == 0:
            return
        sleep(1)


def ocf_monitor():
    pgisready_rc = run_pgisready()
    if pgisready_rc == 0:
        log_debug("PG is listening, checking if it is a master or standby")
        status = is_master_or_standby()
        if status == OCF_RUNNING_MASTER:
            set_standbies_scores()
        return status
    if pgisready_rc == 1:
        # PG rejects connections. In normal circumstances, this should only
        # happen on a standby that was just started until it has completed
        # sufficient recovery to provide a consistent state against which
        # queries can run
        log_warn("PG server is running but refuses connections. "
                 "This should only happen on a standby that was just started.")
        loop_until_pgisready()
        if is_master_or_standby() == OCF_SUCCESS:
            log_info("PG now accepts connections, and it is a standby")
            return OCF_SUCCESS
        log_err("PG now accespts connections, but it is not a standby")
        return OCF_ERR_GENERIC
    if pgisready_rc == 2:  # PG is not listening.
        log_info("pg_isready says PG is not listening; 'pg_ctl status' will "
                 "tell if the server is running")
        if pg_ctl_status() == 0:
            log_info("'pg_ctl status' says the server is running")
            log_err("PG is not listening, but the server is running")
            return OCF_ERR_GENERIC
        log_info("'pg_ctl status' says the server is not running")
        if backup_label_exists():
            log_debug("found backup_label: indicates a never started backup")
            return OCF_NOT_RUNNING
        log_debug("no backup_label found: let's check pg_controldata's state "
                  "to infer if the shutdown was graceful")
        state = get_pgctrldata_state()
        log_info("pg_controldata's state is: {}", state)
        if state in ("shut down",  # was a Master
                     "shut down in recovery"  # was a standby
                     ):
            log_debug("shutdown was graceful, all good")
            return OCF_NOT_RUNNING
        log_err("shutdown was not graceful")
        return OCF_ERR_GENERIC
    log_err("unexpected pg_isready exit code {} ", pgisready_rc)
    return OCF_ERR_GENERIC


def get_ocf_state():
    pgisready_rc = run_pgisready()
    if pgisready_rc == 0:
        log_debug("PG is listening")
        return is_master_or_standby()
    if pgisready_rc == 1:
        # The attempt was rejected.
        # There are different reasons for this:
        #   - startup in progres
        #   - shutdown in progress
        #   - in crash recovery
        #   - instance is a warm standby
        # Except for the warm standby case, this should be a transitional state.
        # We try to confirm using pg_controldata.
        log_debug("PG rejects connections, checking again...")
        controldata_rc = get_non_transitional_pg_state()
        if controldata_rc in (OCF_RUNNING_MASTER, OCF_SUCCESS):
            # This state indicates that pg_isready check should succeed.
            # We check again.
            log_debug("PG's controldata shows a running status")
            pgisready_rc = run_pgisready()
            if pgisready_rc == 0:
                # Consistent with pg_controdata output.
                # We can check if the instance is master or standby
                log_debug("PG is listening")
                return is_master_or_standby()
            # Still not consistent, raise an error.
            # NOTE: if the instance is a warm standby, we end here.
            # TODO raise a hard error here ?
            log_err("PG's controldata is not consistent with pg_isready "
                    "(returned: {})", pgisready_rc)
            log_info("if this instance is in warm standby, "
                     "this resource agent only supports hot standby")
            return OCF_ERR_GENERIC
        if controldata_rc == OCF_NOT_RUNNING:
            # This state indicates that pg_isready check should fail with rc 2.
            # We check again.
            pgisready_rc = run_pgisready()
            if pgisready_rc == 2:
                # Consistent with pg_controdata output.
                # We check the process status using pg_ctl status and check
                # if it was propertly shut down using pg_controldata.
                log_debug("PG is not listening")
                return confirm_stopped()
            # Still not consistent, raise an error.
            # TODO raise an hard error here ?
            log_err("PG's controldata is not consistent "
                    "with pg_isready (returned: {})", pgisready_rc)
            return OCF_ERR_GENERIC
        # Something went wrong with the controldata check, hard fail.
        log_err("could not get PG's status from "
                "controldata (returned: {})", controldata_rc)
        return OCF_ERR_INSTALLED
    elif pgisready_rc == 2:
        # The instance is not listening.
        # We check the process status using pg_ctl status and check
        # if it was propertly shut down using pg_controldata.
        log_debug("PG is not listening")
        return confirm_stopped()
    elif pgisready_rc == 3:
        # No attempt was done, probably a syntax error.
        # Hard configuration error, we don't want to retry or failover here.
        log_err("unknown error while checking if PG "
                "is listening (returned {})", pgisready_rc)
        return OCF_ERR_CONFIGURED
    log_err("unexpected pg_isready status")
    return OCF_ERR_GENERIC


def ocf_demote():
    """ Demote PG from master to standby: stop PG, create recovery.conf and
    start it back """
    rc = get_ocf_state()
    if rc == OCF_RUNNING_MASTER:
        log_debug("PG running as a master")
    elif rc == OCF_SUCCESS:
        log_debug("PG running as a standby")
        return OCF_SUCCESS
    elif rc == OCF_NOT_RUNNING:
        log_debug("PG already stopped stopped")
    elif rc == OCF_ERR_CONFIGURED:
        # We actually prefer raising a hard or fatal error instead of leaving
        # the CRM abording its transition for a new one because of a soft error.
        # The hard error will force the CRM to move the resource immediately.
        return OCF_ERR_CONFIGURED
    else:
        return OCF_ERR_GENERIC
    # TODO we need to make sure at least one standby is connected!!
    # WARNING if the resource state is stopped instead of master, the ocf ra dev
    # rsc advises to return OCF_ERR_GENERIC, misleading the CRM in a loop where
    # it computes transitions of demote(failing)->stop->start->promote actions
    # until failcount == migration-threshold.
    # This is a really ugly trick to keep going with the demode action if the
    # rsc is already stopped gracefully.
    # See discussion "CRM trying to demote a stopped resource" on
    # developers@clusterlabs.org
    if rc != OCF_NOT_RUNNING:
        # insanely long timeout to ensure Pacemaker gives up firt
        rc = pg_ctl_stop()
        if rc != 0:
            log_err("failed to stop PG (pg_ctl exited with {})", rc)
            return OCF_ERR_GENERIC
        # Double check that PG is stopped correctly
        rc = get_ocf_state()
        if rc != OCF_NOT_RUNNING:
            log_err("unexpected state: monitor status "
                    "({}) disagree with pg_ctl return code", rc)
            return OCF_ERR_GENERIC
    # At this point, PG MUST be stopped gracefully. Start PG back as a standby.
    rc = start_pg()
    if rc == OCF_SUCCESS:
        log_info("PG started as a standby")
        return OCF_SUCCESS
    log_err("failed to start PG as standby (returned {})", rc)
    return OCF_ERR_GENERIC


def get_notify_dict():
    d = OrderedDict()
    d["type"] = os.environ.get("OCF_RESKEY_CRM_meta_notify_type", "")
    d["operation"] = os.environ.get("OCF_RESKEY_CRM_meta_notify_operation", "")
    d["nodes"] = defaultdict(list)
    start, end = "OCF_RESKEY_CRM_meta_notify_", "_uname"
    for k, v in os.environ.items():
        if k.startswith(start) and k.endswith(end):
            action = k[len(start):-len(end)]
            if v.strip():
                d["nodes"][action].extend(v.split())
    return d


def ocf_notify():
    d = get_notify_dict()
    log_debug("{}", json.dumps(d, indent=4))
    type_op = d["type"] + "-" + d["operation"]
    if type_op == "pre-promote":
        return notify_pre_promote(d["nodes"])
    if type_op == "post-promote":
        del_private_attributes()
        return OCF_SUCCESS
    if type_op == "pre-demote":
        return notify_pre_demote(d["nodes"])
    if type_op == "pre-stop":
        return notify_pre_stop(d["nodes"])
    if type_op == "pre-start":
        return notify_pre_start(d["nodes"])
    return OCF_SUCCESS


if __name__ == "__main__":
    if ACTION == "meta-data":
        print(OCF_META_DATA)
        sys.exit()
    if ACTION == "methods":
        print(OCF_METHODS)
        sys.exit()
    os.chdir(gettempdir())
    if int(os.environ.get("OCF_RESKEY_CRM_meta_clone_max", 0)) <= 0:
        log_err("OCF_RESKEY_CRM_meta_clone_max should be >= 1")
        sys.exit(OCF_ERR_CONFIGURED)
    if os.environ.get("OCF_RESKEY_CRM_meta_master_max", "") != "1":
        log_err("OCF_RESKEY_CRM_meta_master_max should == 1")
        sys.exit(OCF_ERR_CONFIGURED)
    if ACTION == "validate-all":
        sys.exit(ocf_validate_all())
    if ACTION in ("start", "stop", "monitor", "promote", "demote", "notify"):
        ocf_validate_all()
        if ACTION == "start":  # start as standby
            sys.exit(ocf_start())
        if ACTION == "stop":
            sys.exit(ocf_stop())
        if ACTION == "monitor":
            sys.exit(ocf_monitor())
        if ACTION == "promote":  # makes it a master
            sys.exit(ocf_promote())
        if ACTION == "demote":
            sys.exit(ocf_demote())
        if ACTION == "notify":
            sys.exit(ocf_notify())
    else:
        sys.exit(OCF_ERR_UNIMPLEMENTED)
