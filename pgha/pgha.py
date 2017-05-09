#!/usr/bin/python2.7
from __future__ import division, print_function, unicode_literals
import os
import json
import pwd
import re
import sys
import tempfile
from datetime import datetime
from itertools import chain
from subprocess import call, check_output, CalledProcessError, STDOUT
from collections import defaultdict, namedtuple, OrderedDict
from distutils.version import LooseVersion
from functools import partial
from tempfile import gettempdir
from time import sleep

VERSION = "1.0"
PROGRAM = "pgha"
MIN_PG_VER = LooseVersion('9.5')
OCF_SUCCESS = 0
OCF_RUNNING_SLAVE = OCF_SUCCESS
OCF_ERR_GENERIC = 1
OCF_ERR_ARGS = 2
OCF_ERR_UNIMPLEMENTED = 3
OCF_ERR_PERM = 4
OCF_ERR_INSTALLED = 5
OCF_ERR_CONFIGURED = 6
OCF_NOT_RUNNING = 7
OCF_RUNNING_MASTER = 8
OCF_FAILED_MASTER = 9
RE_WAL_LEVEL = r"^wal_level setting:\s+(.*?)\s*$"
RE_TPL_TIMELINE = r"^\s*recovery_target_timeline\s*=\s*'?latest'?\s*$"
RE_STANDBY_MODE = r"^\s*standby_mode\s*=\s*'?on'?\s*$"
RE_APP_NAME = r"^\s*primary_conninfo\s*=.*['\s]application_name=(?P<n>.*?)['\s]"
RE_PG_CLUSTER_STATE = r"^Database cluster state:\s+(.*?)\s*$"
pguser_default = "postgres"
bindir_default = "/usr/bin"
pgdata_default = "/var/lib/pgsql/data"
pghost_default = "/var/run/postgresql"
pgport_default = "5432"
OCF_META_DATA = """\
<?xml version="1.0"?>
<!DOCTYPE resource-agent SYSTEM "ra-api-1.dtd">
<resource-agent name="pgsqlha">
<version>1.0</version>
<longdesc lang="en">Resource script for replicated PostgreSQL. It manages 
PostgreSQL instances using streaming replication as an HA resource.</longdesc>
<shortdesc lang="en">Manages replicated PostgreSQL instances</shortdesc>
<parameters>
    <parameter name="pguser" unique="0" required="0">
    <longdesc lang="en">PostgreSQL server user</longdesc>
    <shortdesc lang="en">PostgreSQL server user</shortdesc>
    <content type="string" default="{pguser_default}" />
</parameter>
<parameter name="bindir" unique="0" required="0">
    <longdesc lang="en">Directory with PostgreSQL binaries. This RA uses 
    psql, pg_isready, pg_controldata and pg_ctl.</longdesc>
    <shortdesc lang="en">Path to PostgreSQL binaries</shortdesc>
    <content type="string" default="{bindir_default}" />
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
_rcs_inst = None


def get_rcs_inst():
    global _rcs_inst
    if not _rcs_inst:
        _rcs_inst = os.environ['OCF_RESOURCE_INSTANCE']
    return _rcs_inst


def hadate():
    return datetime.now().strftime(os.environ.get('HA_DATEFMT', '%Y/%m/%d_%T'))


def get_pguser():
    return os.environ.get('OCF_RESKEY_pguser', pguser_default)


def get_bindir():
    return os.environ.get('OCF_RESKEY_bindir', bindir_default)


def get_pgdata():
    return os.environ.get('OCF_RESKEY_pgdata', pgdata_default)


def get_pghost():
    return os.environ.get('OCF_RESKEY_pghost', pghost_default)


def get_pgport():
    return os.environ.get('OCF_RESKEY_pgport', pgport_default)


def get_recovery_pcmk():
    return os.path.join(get_pgdata(), "recovery.conf.pcmk")


def get_pgctl():
    return os.path.join(get_bindir(), "pg_ctl")


def get_psql():
    return os.path.join(get_bindir(), "psql")


def get_pgctrldata():
    return os.path.join(get_bindir(), "pg_controldata")


def get_pgisready():
    return os.path.join(get_bindir(), "pg_isready")


def get_ha_bin():
    return os.environ.get('HA_SBIN_DIR', '/usr/sbin')


def get_ha_debuglog():
    return os.environ.get('HA_DEBUGLOG', "")


def get_crm_master():
    return os.path.join(get_ha_bin(), "crm_master") + " --lifetime forever"


def get_crm_node():
    return os.path.join(get_ha_bin(), "crm_node")


def get_pacemakerd():
    return os.path.join(get_ha_bin(), "pacemakerd")


def as_postgres_user():
    u = pwd.getpwnam(get_pguser())
    os.initgroups(get_pguser(), u.pw_gid)
    os.setgid(u.pw_gid)
    os.setuid(u.pw_uid)
    os.seteuid(u.pw_uid)


def get_logtag():
    global _logtag
    if not _logtag:
        _logtag = "{}:{}({})[{}]".format(
            PROGRAM, ACTION, get_rcs_inst(), os.getpid())
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


# see https://www.postgresql.org/docs/current/static/app-pg-isready.html
def run_pgisready():
    return as_postgres([get_pgisready(), "-h", get_pghost(), "-p", get_pgport()])


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


def get_connected_standbies():
    # We check locations of connected standbies by querying the
    # "pg_stat_replication" view.
    # The row_number applies on the result set ordered on write_location ASC so
    # the highest row_number should be given to the closest node from the
    # master, then the lowest node name (alphanumeric sort) in case of equality.
    # The result set itself is order by priority DESC to process best known
    # candidate first.
    query = """\
SELECT application_name, priority, location, state
FROM (
    SELECT application_name,
    1000 - (
        row_number() OVER (
            PARTITION BY state IN ('startup', 'backup')
            ORDER BY write_location ASC, application_name ASC
        ) - 1
    ) * 10 AS priority,
    write_location AS location, state
    FROM (
        SELECT application_name, write_location, state
        FROM pg_stat_replication
    ) AS s2
) AS s1
ORDER BY priority DESC"""
    rc, rs = pg_execute(query)
    if rc != 0:
        log_err("query to get standby locations failed ({})", rc)
        sys.exit(OCF_ERR_GENERIC)
    if not rs:
        log_warn("no standby connected")
    nt = namedtuple("nt", "application_name priority location state")
    return [nt(*r) for r in rs]


# Check the write_location of secondaries, and adapt their master score so
# that the instance closest to the master will be the selected candidate should
# a promotion be triggered.
# NOTE: This is only a hint to pacemaker! The selected candidate to promotion
# actually re-checks that it is the best candidate and forces a re-election by
# failing if a better one exists.
# This avoid a race condition between the call of the monitor action and the
# promotion where another standby might have catchup faster with the master.
# NOTE: we cannot directly use write_location, nor lsn_diff as promotion score
# because Pacemaker considers any value greater than 1,000,000 to be INFINITY.
#
# Supposed to be called from a master monitor action.
def check_locations():
    nodename = get_ocf_nodename()
    # exclude nodes that are not part of the cluster
    crm_nodes = get_ha_nodes()
    # For each standby connected, set their master score based on the following
    # rule: the first known node/application, with the highest priority and
    # with an acceptable state.
    for standby in get_connected_standbies():
        if standby.application_name not in crm_nodes:
            log_info("ignoring unknown application_name/node {}",
                     standby.application_name)
            continue
        if standby.application_name == nodename:
            log_warn("streaming replication with myself!")
            continue
        node_score = get_master_score(standby.application_name)
        if standby.state.strip() in ("startup", "backup"):
            # We exclude any standby being in state backup (pg_basebackup) or
            # startup (new standby or failing standby)
            log_info("forbid promotion on {} in state '{}', set score to -1",
                     standby.application_name, standby.state)
            if node_score != "-1":
                set_master_score("-1", standby.application_name)
        else:
            log_debug("checking {} promotion ability (current_score: {}, "
                      "priority: {}, location: {})", standby.application_name,
                      node_score, standby.priority, standby.location)
            if node_score != standby.priority:
                log_info("update score of {} from {} to {}",
                         standby.application_name, node_score, standby.priority)
                set_master_score(standby.priority, standby.application_name)
            else:
                log_debug("{} keeps its current score of {}",
                          standby.application_name, standby.priority)
        # Remove this node from the known nodes list.
        crm_nodes.remove(standby.application_name.strip())
    # If there are still nodes in "crm_nodes", it means there is no
    # corresponding line in "pg_stat_replication".
    for node in crm_nodes:
        # Exclude the current node.
        if node == nodename:
            continue
        log_warn("{} is not connected to the master, set score to -1000", node)
        set_master_score("-1000", node)
    # Finally set the master score if not already done
    node_score = get_master_score()
    if node_score != "1001":
        set_master_score("1001")
    return OCF_SUCCESS


# Confirm if the instance is really started as pgisready stated and
# if the instance is master or standby.
def confirm_role():
    inst = get_rcs_inst()
    rc, rs = pg_execute("SELECT pg_is_in_recovery()")
    is_in_recovery = rs[0][0]
    if rc == 0:
        # The query was executed, check the result.
        if is_in_recovery == "t":
            log_debug("{} is a standby", inst)
            return OCF_SUCCESS
        elif is_in_recovery == "f":
            log_debug("{} is a master", inst)
            # Check lsn diff with current slaves if any
            if ACTION == "monitor":
                check_locations()
            return OCF_RUNNING_MASTER
        # This should not happen, raise a hard configuration error.
        log_err("unexpected result from query to check if {}\" "
                "is a master or a standby: '{}'", inst, is_in_recovery)
        return OCF_ERR_CONFIGURED
    elif rc in (1, 2):
        # psql cound not connect to the instance.
        # As pg_isready reported the instance was listening, this error
        # could be a max_connection saturation. Just report a soft error.
        log_err("psql can't connect to {}", inst)
        return OCF_ERR_GENERIC
    # The query failed (rc: 3) or bad parameters (rc: -1).
    # This should not happen, raise a hard configuration error.
    log_err(
        "query to check if {} is a master or standby failed (rc: {})", inst, rc)
    return OCF_ERR_CONFIGURED


# Parse and return the current status of the local PostgreSQL instance as
# reported by its controldata file
# WARNING: the status is NOT updated in case of crash.
def get_pg_cluster_state():
    datadir = get_pgdata()
    finds = re.findall(RE_PG_CLUSTER_STATE, run_pgctrldata(), re.M)
    if not finds:
        log_crit("couldn't read state from controldata file for {}", datadir)
        sys.exit(OCF_ERR_CONFIGURED)
    log_debug("state of {} is '{}'", get_rcs_inst(), finds[0])
    return finds[0]


# Loop until pg_controldata returns a non-transitional state
# When this happens: return an OCF status based on the cluster state.
# Used to find out if this instance is a master or standby.
# Also used to detect if the instance has crashed.
def get_ocf_status():
    inst = get_rcs_inst()
    while True:
        state = get_pg_cluster_state()
        if state == "":
            # Something went really wrong with pg_controldata.
            log_err("get_ocf_status: no PG cluster state for {}", inst)
            sys.exit(OCF_ERR_INSTALLED)
        if state == "in production":
            return OCF_RUNNING_MASTER  # is a master
        # This state includes warm standby (rejects connections attempts,
        # including pg_isready)
        if state == "in archive recovery":
            return OCF_SUCCESS  # is a standby
        # The instance should be stopped.
        # We don't care if it was a master or standby before, because we
        # always start instances as secondaries, and then promote if necessary.
        if state in ("shut down", "shut down in recovery"):
            return OCF_NOT_RUNNING  # is stopped
        # The state is "in crash recovery", "starting up" or "shutting down".
        # This state should be transitional, so we wait and loop to check if
        # it changes.
        # If it does not, pacemaker will eventually abort with a timeout.
        log_debug("get_ocf_status: waiting for "
                  "transitionnal state '{}' to change", state)
        sleep(1)


# Check the postmaster.pid file and the postmaster process.
# WARNING: doesn't distinguish a missing postmaster.pid from a process that is
# still alive. This is fine: monitor will find this to be a hard error.
def pg_ctl_status():
    rc = as_postgres([get_pgctl(), "status", "-D", get_pgdata()])
    # pg_ctl status exits with 3 when postmaster.pid does not exist or the
    # process with the PID is not alive (otherwise it returns 0)
    return rc


def pg_ctl_start():
    # insanely long timeout to ensure Pacemaker gives up first
    conf = os.environ.get(
        "OCF_RESKEY_pgconf", os.path.join(get_pgdata(), "postgresql.conf"))
    cmd = [get_pgctl(), "start", "-D", get_pgdata(), "-w", "-t", 1000000,
           "-o", "-c config_file=" + conf]
    return as_postgres(cmd)


def pg_ctl_stop():
    return as_postgres([get_pgctl(), "stop", "-D", get_pgdata(),
                        "-w", "-t", 1000000, "-m", "fast"])


# PG is really stopped as pgisready stated and it was propertly shut down
def confirm_stopped():
    inst = get_rcs_inst()
    # Check the postmaster process status.
    pgctlstatus_rc = pg_ctl_status()
    if pgctlstatus_rc == 0:
        # The PID file exists and the process is available.
        # That should not be the case, return an error.
        log_err("{} is not listening, but the process in postmaster.pid exists",
                inst)
        return OCF_ERR_GENERIC
    # The PID file does not exist or the process is not available.
    log_debug("no postmaster process found for {}", inst)
    backup_lbl = os.path.join(get_pgdata(), "backup_label")
    if os.path.isfile(backup_lbl):
        log_debug("found {}: probably a never started standby", backup_lbl)
        return OCF_NOT_RUNNING
    # Continue the check with pg_controldata.
    controldata_rc = get_ocf_status()
    if controldata_rc == OCF_RUNNING_MASTER:
        # The controldata has not been updated to "shutdown".
        # It should mean we had a crash on a master instance.
        log_err("{}'s controldata indicates a running "
                "master, the instance has probably crashed", inst)
        return OCF_FAILED_MASTER
    elif controldata_rc == OCF_SUCCESS:
        # The controldata has not been updated to "shutdown in recovery".
        # It should mean we had a crash on a standby instance.
        # There is no "FAILED_SLAVE" return code, so we return a generic error.
        log_warn(
            "{} appears to be a crashed standby, let's pretend all is good "
            "so that Pacemaker tries to start it back as-is", inst)
        return OCF_NOT_RUNNING
    elif controldata_rc == OCF_NOT_RUNNING:
        # The controldata state is consistent, the instance was probably
        # propertly shut down.
        log_debug("{}'s controldata indicates "
                  "that the instance was propertly shut down", inst)
        return OCF_NOT_RUNNING
    # Something went wrong with the controldata check.
    log_err("couldn't get {}'s status from controldata (returned {})",
            inst, controldata_rc)
    return OCF_ERR_GENERIC


# Create the recovery file based on the given template.
# Given template MUST at least contain:
#   standby_mode=on
#   primary_conninfo='...'
#   recovery_target_timeline = 'latest'
def create_recovery_conf():
    u = pwd.getpwnam(get_pguser())
    uid, gid = u.pw_uid, u.pw_gid
    recovery_file = os.path.join(get_pgdata(), "recovery.conf")
    recovery_tpl = get_recovery_pcmk()
    log_debug("get replication configuration from "
              "the template file {}", recovery_tpl)
    # Create the recovery.conf file to start the instance as a standby.
    # NOTE: the recovery.conf is supposed to be set up so the standby can
    # connect to the master instance, usually using a virtual IP address.
    # As there is no master instance available at startup, secondaries will
    # complain about failing to connect.
    # As we can not reload a recovery.conf file on a standby without restarting
    # it, we will leave with this.
    # FIXME how would the reload help us in this case ?
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


# Get the resource master score of a node
def get_master_score(node=None):
    cmd = [get_crm_master(), "--quiet", "--get-value"]
    if node:
        cmd.extend(["-N", node])
    try:
        score = check_output(" ".join(cmd), shell=True)
    except CalledProcessError:
        return ""
    return score.strip()


# Check if a master score is set for one of the relative clones
# in the cluster and the score is greater or equal of 0.
# Returns True if at least one master score >= 0 is found, False otherwise
def _master_score_exists():
    crm_nodes = get_ha_nodes()
    for node in crm_nodes:
        score = get_master_score(node)
        if score != "" and int(score) > -1:
            return True
    return False


# Setting attributes is asynchronous, so return as soon as truly done
def set_master_score(score, node=None):
    cmd = [get_crm_master(), "-q", "-v", score]
    if node:
        cmd.extend(["-N", node])
    call(" ".join(cmd), shell=True)
    while True:
        tmp = get_master_score(node)
        if tmp == score:
            break
        log_debug("waiting to set score to {} (currently {})...", score, tmp)
        sleep(0.1)


def ocf_promote():
    inst = get_rcs_inst()
    rc = ocf_monitor()
    if rc == OCF_SUCCESS:
        # Running as standby. Normal, expected behavior.
        log_debug("{} running as a standby", inst)
    elif rc == OCF_RUNNING_MASTER:
        # Already a master. Unexpected, but not a problem.
        log_info("{} running as a master", inst)
        return OCF_SUCCESS
    elif rc == OCF_NOT_RUNNING:  # INFO this is not supposed to happen.
        # Currently not running. Need to start before promoting.
        log_info("{} stopped, starting it", inst)
        rc = ocf_start()
        if rc != OCF_SUCCESS:
            log_err("failed to start {}", inst)
            return OCF_ERR_GENERIC
    else:
        log_info("unexpected error, cannot promote {}", inst)
        return OCF_ERR_GENERIC
    #
    # At this point, the instance **MUST** be started as a standby.
    #
    # Cancel the switchover if it has been considered unsafe during pre-promote
    if get_ha_private_attr("cancel_switchover") == "1":
        log_err("switch from pre-promote action canceled")
        del_ha_private_attr("cancel_switchover")
        return OCF_ERR_GENERIC
    # Do not check for a better candidate if we try to recover the master
    # Recovery of a master is detected during pre-promote. It sets the
    # private attribute 'recover_master' to '1' if this is a master recovery.
    if get_ha_private_attr("recover_master") == "1":
        log_info("recovering old master, no election needed")
    else:
        if not run_election():
            return OCF_ERR_GENERIC
    if not add_replication_slots(get_ha_nodes()):
        log_err("failed to add all replication slots", inst)
        return OCF_ERR_GENERIC
    if as_postgres([get_pgctl(), "promote", "-D", get_pgdata(), "-w", ]) != 0:
        # Promote the instance on the current node.
        log_err("error during promotion")
        return OCF_ERR_GENERIC
    # promote is asynchronous: wait for it to finish
    while ocf_monitor() != OCF_RUNNING_MASTER:
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
            continue
        slot = node.replace('-', '_') + "_slot"
        if slot in slots:
            continue
        rc, rs = pg_execute(
            "SELECT * FROM pg_create_physical_replication_slot('{}')".format(
                slot))
        if rc != 0:
            log_err("failed to add replication slot {}".format(slot))
            return False
        log_debug("added replication slot {}".format(slot))
    return True


def delete_replication_slots():
    rc, rs = pg_execute("SELECT slot_name FROM pg_replication_slots")
    if not rs:
        log_debug("no replication slots to delete")
        return True
    for slot in [r[0] for r in rs]:
        q = "SELECT * FROM pg_drop_replication_slot('{}')".format(slot)
        rc, rs = pg_execute(q)
        if rc != 0:
            log_err("failed to delete replication slot {}".format(slot))
            return False
        log_debug("deleted replication slot {}".format(slot))
    return True


def run_election():
    # The slave with the highest master score gets promoted, as chosen by
    # pacemaker during the last monitor on previous master (see
    # ocf_monitor/check_locations subs). To avoid a race condition between the
    # last monitor action on the previous master and the real most up-to-date
    # standby, we set each standby location during pre-promote, and store them
    # using the "lsn_location" resource attribute.
    #
    # The best standby to promote has the highest LSN. If the current resource
    # is not the best one, we need to modify the master scores accordingly,
    # and abort the current promotion.
    log_debug("checking if current node is the best for promotion")
    # Exclude nodes that are known to be unavailable (not in the current
    # partition) using the "crm_node" command
    active_nodes = get_ha_private_attr("nodes").split()
    node_to_promote = ""
    # Get the lsn_location attribute value of the current node, as set
    # during the "pre-promote" action.
    # It should be the greatest among the standby instances.
    max_lsn = get_ha_private_attr("lsn_location")
    if max_lsn == "":
        # Should not happen since the "lsn_location" attribute should have
        # been updated during pre-promote.
        log_crit("can not get current node LSN location")
        return False
    # convert location to decimal
    max_lsn = max_lsn.strip("\n")
    wal_num, wal_off = max_lsn.split("/")
    max_lsn_dec = (294967296 * int(wal_num, 16)) + int(wal_off, 16)
    log_debug("current node lsn location: {}({})", max_lsn, max_lsn_dec)
    # Now we compare with the other available nodes.
    nodename = get_ocf_nodename()
    for node in active_nodes:
        # We exclude the current node from the check.
        if node == nodename:
            continue
        # Get the "lsn_location" attribute value for the node, as set during
        # the "pre-promote" action.
        node_lsn = get_ha_private_attr("lsn_location", node)
        if node_lsn == "":
            # This should not happen as the "lsn_location" attribute should
            # have been updated during the "pre-promote" action.
            log_crit("can't get LSN location of {}", node)
            return False
        # convert location to decimal
        node_lsn = node_lsn.strip("\n")
        wal_num, wal_off = node_lsn.split("/")
        node_lsn_dec = (4294967296 * int(wal_num, 16)) + int(wal_off, 16)
        log_debug("comparing with {}: lsn is {}({})",
                  node, node_lsn, node_lsn_dec)
        # If the node has a bigger delta, select it as a best candidate to
        # promotion.
        if node_lsn_dec > max_lsn_dec:
            node_to_promote = node
            max_lsn_dec = node_lsn_dec
            # max_lsn = node_lsn
            log_debug("found {} as a better candidate to promote", node)
    # If any node has been selected, we adapt the master scores accordingly
    # and break the current promotion.
    if node_to_promote != "":
        log_info("{} is the best candidate to promote, "
                 "aborting current promotion", node_to_promote)
        # Reset current node master score.
        set_master_score("1")
        # Set promotion candidate master score.
        set_master_score("1000", node_to_promote)
        # Make the promotion fail to trigger another promotion transition
        # with the new scores.
        return False
    # keep on promoting the current node.
    return True


# check if th current transition is a switchover to the given node
def is_switchover(nodes, n):
    old = None
    if nodes["master"]:
        old = nodes["master"][0]
    if (len(nodes["master"]) != 1 or
            len(nodes["demote"]) != 1 or len(nodes["promote"]) != 1):
        return False
    t1 = old in nodes["demote"]
    t4 = old in nodes["stop"]
    t2 = n in nodes["slave"]
    t3 = n in nodes["promote"]
    return t1 and t2 and t3 and not t4


# check if the pgsql switchover to the localnode is safe.
# This is supposed to be called **after** the master has been stopped or demote.
# This sub check if the local standby received the shutdown checkpoint from the
# old master to make sure it can take over the master role and the old master
# will be able to catchup as a standby after.
#
# Returns 0 if switchover is safe
# Returns 1 if swithcover is not safe
# Returns 2 for internal error
def check_switchover(nodes):
    log_info("switch from {} to {} in progress, checking last record in WAL",
             nodes["demote"][0], get_ocf_nodename())
    # Force a checpoint to make sure the controldata shows the very last TL
    pg_execute("CHECKPOINT")
    # check if we received the shutdown checkpoint of the master during its
    # demote process.
    # We need the last local checkpoint LSN and the last received LSN from
    # master to check in the WAL between these adresses if we have a
    # "checkpoint shutdown" using pg_xlogdump.
    datadir = get_pgdata()
    ans = run_pgctrldata()
    # Get the latest known TL
    p = re.compile(r"^Latest checkpoint's TimeLineID:\s+(\d+)\s*$", re.M)
    m = p.findall(ans)
    tl = m[0] if m else None
    # Get the latest local checkpoint
    p = re.compile(
        r"^Latest checkpoint's REDO location:\s+([0-9A-F/]+)\s*$", re.M)
    m = p.findall(ans)
    last_chk = m[0] if m else None
    # Get the last received LSN from master
    #  0/4000000
    rc, rs = pg_execute("SELECT pg_last_xlog_receive_location()")
    if rc != 0:
        log_err("could not query last_xlog_receive_location ({})", rc)
        return 2
    last_lsn = rs[0][0] if rs and rs[0] else None
    if not (tl and last_chk and last_lsn):
        log_crit("couldn't read last checkpoint or timeline from controldata")
        log_debug("XLOGDUMP parameters:  datadir: {}, last_chk: {}, tl: {}, "
                  "mast_lsn: {}", datadir, last_chk, tl, last_lsn)
        return 2
    # force checkpoint on the standby to flush the master's
    # shutdown checkpoint to WAL
    rc = 0
    try:
        ans = check_output([
            os.path.join(get_bindir(), "pg_xlogdump"), "-p", datadir, "-t", tl,
            "-s", last_chk, "-e", last_lsn], stderr=STDOUT)
    except CalledProcessError as e:
        rc = e.returncode
        ans = e.output
    log_debug("XLOGDUMP rc: {}, tl: {}, last_chk: {}, last_lsn: {}, "
              "output: [{}]", rc, tl, last_chk, last_lsn, ans)
    p = re.compile(
        r"^rmgr: XLOG.*desc: [cC][hH][eE][cC][kK][pP][oO][iI][nN][tT]"
        r"(:|_SHUTDOWN) redo [0-9A-F/]+; tli " + tl + r";.*; shutdown$",
        re.M | re.DOTALL)
    if rc == 0 and p.search(ans):
        log_info("standby received shutdown checkpoint", get_pg_cluster_state())
        return 0
    set_ha_private_attr("cancel_switchover", "1")
    log_info("did not received shutdown checkpoint from old master")
    return 1


def notify_pre_promote(nodes):
    node = get_ocf_nodename()
    promoting = nodes["promote"][0]
    log_info("pre_promote: promoting instance on {}", promoting)
    # No need to do an election between slaves if this is recovery of the master
    if promoting in nodes["master"]:
        log_warn("pre_promote: this is a master recovery")
        if promoting == node:
            set_ha_private_attr("recover_master", "1")
        return OCF_SUCCESS
    # Environment cleanup!
    del_ha_private_attr("lsn_location")
    del_ha_private_attr("recover_master")
    del_ha_private_attr("nodes")
    del_ha_private_attr("cancel_switchover")
    # check for the last received entry of WAL from the master if we are
    # the designated standby to promote
    if is_switchover(nodes, node) and node in nodes["promote"]:
        rc = check_switchover(nodes)
        if rc == 1:
            # Shortcut the election process because the switchover will be
            # canceled
            return OCF_SUCCESS
        elif rc != 0:
            # This is an extreme mesure, it shouldn't happen.
            call([os.path.join(get_ha_bin(), "crm_failcount"),
                  "-r", get_rcs_inst(), "-v", "1000000"])
            return OCF_ERR_INSTALLED
            # If the sub keeps going, that means the switchover is safe.
            # Keep going with the election process in case the switchover was
            # instruct to the wrong node.
            # FIXME: should we allow a switchover to a lagging standby?
    # We need to trigger an election between existing slaves to promote the best
    # one based on its current LSN location. The designated standby for
    # promotion is responsible to connect to each available nodes to check their
    # "lsn_location".
    # During the following promote action, ocf_promote will use this
    # information to check if the instance to be promoted is the best one,
    # so we can avoid a race condition between the last successful monitor
    # on the previous master and the current promotion.
    rc, rs = pg_execute("SELECT pg_last_xlog_receive_location()")
    if rc != 0:
        log_warn("pre_promote: could not query the current node LSN")
        # Return code are ignored during notifications...
        return OCF_SUCCESS
    node_lsn = rs[0][0]
    log_info("pre_promote: current node LSN: {}", node_lsn)
    # Set the "lsn_location" attribute value for this node so we can use it
    # during the following "promote" action.
    if not set_ha_private_attr("lsn_location", node_lsn):
        log_warn("pre_promote: could not set the current node LSN")
    # If this node is the future master, keep track of the slaves that
    # received the same notification to compare our LSN with them during
    # promotion
    active_nodes = defaultdict(int)
    if promoting == node:
        # build the list of active nodes:
        #   master + slave + start - stop
        for uname in chain(nodes["master"], nodes["slave"], nodes["start"]):
            active_nodes[uname] += 1
        for uname in nodes["stop"]:
            active_nodes[uname] -= 1
        attr_nodes = " ".join(k for k in active_nodes if active_nodes[k] > 0)
        set_ha_private_attr("nodes", attr_nodes)
    return OCF_SUCCESS


def notify_post_promote():
    # We have a new master (or the previous one recovered).
    del_ha_private_attr("lsn_location")
    del_ha_private_attr("recover_master")
    del_ha_private_attr("nodes")
    del_ha_private_attr("cancel_switchover")
    return OCF_SUCCESS


def notify_pre_demote(nodes):
    # do nothing if the local node will not be demoted
    this_node = get_ocf_nodename()
    if this_node not in nodes["demote"]:
        return OCF_SUCCESS
    rc = ocf_monitor()
    # do nothing if this is not a master recovery
    master_recovery = (this_node in nodes["master"] and
                       this_node in nodes["promote"])
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
    log_info("pre_demote: trying to start failing master {}", get_rcs_inst())
    # Either the instance managed to start or it couldn't.
    # We rely on the pg_ctk '-w' switch to take care of this. If it couldn't
    # start, this error will be catched up later during the various checks
    pg_ctl_start()
    log_info("pre_demote: state is '{}' after recovery attempt",
             get_pg_cluster_state())
    return OCF_SUCCESS


def notify_pre_start(nodes):
    """
    If this is a Master, add replication slots for the nodes being started if
    they are absent.
    :param nodes:
    :return: OCF_SUCCESS is all goes well (sole possibility AFAICT)
    """
    if get_ocf_nodename() in nodes["master"]:
        log_info("this node is a Master: add any missing replication slots")
        add_replication_slots(nodes["start"])
    return OCF_SUCCESS


def notify_pre_stop(nodes):
    this_node = get_ocf_nodename()
    # do nothing if the local node will not be stopped
    if this_node not in nodes["stop"]:
        return OCF_SUCCESS
    rc = get_ocf_status()
    # do nothing if this is not a standby recovery
    standby_recovery = (this_node in nodes["slave"] and
                        this_node in nodes["start"])
    if not standby_recovery or rc != OCF_RUNNING_SLAVE:
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
    log_info("pre_stop: trying to start failing standby {}", get_rcs_inst())
    # Either the instance managed to start or it couldn't.
    # We rely on the pg_ctl '-w' switch to take care of this. If it couldn't
    # start, this error will be catched up later during the various checks
    pg_ctl_start()
    log_info("pre_stop: state is '{}' after recovery attempt",
             get_pg_cluster_state())
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
    if not re.search(RE_STANDBY_MODE, content, re.M):
        log_crit(
            "Recovery template file {} must contain 'standby_mode = on'",
            recovery_tpl)
        sys.exit(OCF_ERR_ARGS)
    if not re.search(RE_TPL_TIMELINE, content, re.M):
        log_crit("Recovery template lacks 'recovery_target_timeline = latest'")
        sys.exit(OCF_ERR_ARGS)
    m = re.findall(RE_APP_NAME, content, re.M)
    if not m or not m[0].startswith(get_ocf_nodename()):
        log_crit("Recovery template lacks application_name={} in "
                 "primary_conninfo", get_ocf_nodename())
        sys.exit(OCF_ERR_ARGS)
    # check system user
    try:
        pwd.getpwnam(get_pguser())
    except KeyError:
        log_crit("System user {} does not exist", get_pguser())
        sys.exit(OCF_ERR_ARGS)
    # require wal_level >= hot_standby
    finds = re.findall(RE_WAL_LEVEL, run_pgctrldata(), re.M)
    if not finds:
        log_crit("Could not read wal_level setting")
        sys.exit(OCF_ERR_ARGS)
    if finds[0] not in ("hot_standby", "logical", "replica"):
        log_crit("wal_level must be hot_standby, logical or replica")
        sys.exit(OCF_ERR_ARGS)
    return OCF_SUCCESS


# Start the PostgreSQL instance as a standby
def ocf_start():
    rc = ocf_monitor()
    inst = get_rcs_inst()
    prev_state = get_pg_cluster_state()
    # Instance must be stopped or running as standby
    if rc == OCF_SUCCESS:
        log_info("{} is already started", inst)
        return OCF_SUCCESS
    elif rc != OCF_NOT_RUNNING:
        log_err("unexpected state for {} (returned {})", inst, rc)
        return OCF_ERR_GENERIC
    # From here, the instance is NOT running
    log_debug("{} is stopped, starting it as a standby", inst)
    create_recovery_conf()
    rc = pg_ctl_start()
    if rc != 0:
        log_err("{} failed to start (rc: {})", inst, rc)
        return OCF_ERR_GENERIC
    # Wait for the start to finish.
    while True:
        rc = ocf_monitor()
        if rc != OCF_NOT_RUNNING:
            break
        sleep(1)
    if rc != OCF_SUCCESS:
        log_err("{} is not running as a standby (returned {})", inst, rc)
        return OCF_ERR_GENERIC
    log_info("{} started", inst)
    if not delete_replication_slots():
        log_err("failed to delete all replication slots", inst)
        return OCF_ERR_GENERIC
    # On first cluster start, no master score exists on any standbies unless
    # manually set with crm_master. Without master scores the cluster won't
    # promote any slave. To avoid this, we check if at least one master score
    # exists. Do nothing if it's the case, otherwise set a score of 1 only if
    # the resource was a master shut down before the start.
    if prev_state == "shut down" and not _master_score_exists():
        log_info("no master score around; set mine to 1")
        set_master_score("1")
    return OCF_SUCCESS


# Stop the PostgreSQL instance
def ocf_stop():
    # Instance must be running as standby or master or being stopped.
    # Anything else is an error.
    rc = ocf_monitor()
    inst = get_rcs_inst()
    if rc == OCF_NOT_RUNNING:
        log_info("{} already stopped", inst)
        return OCF_SUCCESS
    elif rc not in (OCF_SUCCESS, OCF_RUNNING_MASTER):
        log_warn("unexpected state for {} (returned {})", inst, rc)
        return OCF_ERR_GENERIC
    # From here, the instance is running for sure.
    log_debug("{} is running, stopping it", inst)
    # Try to quit with proper shutdown.
    # insanely long timeout to ensure Pacemaker gives up first
    if pg_ctl_stop() == 0:
        log_info("{} stopped", inst)
        return OCF_SUCCESS
    log_err("{} failed to stop", inst)
    return OCF_ERR_GENERIC


# Monitor the PostgreSQL instance
def ocf_monitor():
    # First check, verify if the instance is listening.
    pgisready_rc = run_pgisready()
    inst = get_rcs_inst()
    if pgisready_rc == 0:
        # The instance is listening.
        # The instance is up and return if it is a master or a standby
        log_debug("{} is listening", inst)
        return confirm_role()
    if pgisready_rc == 1:
        # The attempt was rejected.
        # This could happen in several cases:
        #   - at startup
        #   - during shutdown
        #   - during crash recovery
        #   - if instance is a warm standby
        # Except for the warm standby case, this should be a transitional state.
        # We try to confirm using pg_controldata.
        log_debug("{} rejects connections, checking again...", inst)
        controldata_rc = get_ocf_status()
        if controldata_rc in (OCF_RUNNING_MASTER, OCF_SUCCESS):
            # This state indicates that pg_isready check should succeed.
            # We check again.
            log_debug("{}'s controldata shows a running status", inst)
            pgisready_rc = run_pgisready()
            if pgisready_rc == 0:
                # Consistent with pg_controdata output.
                # We can check if the instance is master or standby
                log_debug("{} is listening", inst)
                return confirm_role()
            # Still not consistent, raise an error.
            # NOTE: if the instance is a warm standby, we end here.
            # TODO raise an hard error here ?
            log_err("{}'s controldata is not consistent with pg_isready "
                    "(returned: {})", inst, pgisready_rc)
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
                log_debug("{} is not listening", inst)
                return confirm_stopped()
            # Still not consistent, raise an error.
            # TODO raise an hard error here ?
            log_err("controldata of {} is not consistent "
                    "with pg_isready (returned: {})", inst, pgisready_rc)
            return OCF_ERR_GENERIC
        # Something went wrong with the controldata check, hard fail.
        log_err("could not get instance {} status from "
                "controldata (returned: {})", inst, controldata_rc)
        return OCF_ERR_INSTALLED
    elif pgisready_rc == 2:
        # The instance is not listening.
        # We check the process status using pg_ctl status and check
        # if it was propertly shut down using pg_controldata.
        log_debug("instance {} is not listening", inst)
        return confirm_stopped()
    elif pgisready_rc == 3:
        # No attempt was done, probably a syntax error.
        # Hard configuration error, we don't want to retry or failover here.
        log_err("unknown error while checking if {} "
                "is listening (returned {})", inst, pgisready_rc)
        return OCF_ERR_CONFIGURED
    log_err("unexpected pg_isready status for {}", inst)
    return OCF_ERR_GENERIC


# Demote the PostgreSQL instance from master to standby
# To demote a PostgreSQL instance, we must:
#   * stop it gracefully
#   * create recovery.conf with standby_mode = on
#   * start it back
def ocf_demote():
    inst = get_rcs_inst()
    rc = ocf_monitor()
    if rc == OCF_RUNNING_MASTER:
        log_debug("{} running as a master", inst)
    elif rc == OCF_SUCCESS:
        log_debug("{} running as a standby", inst)
        return OCF_SUCCESS
    elif rc == OCF_NOT_RUNNING:
        # Instance is stopped. Nothing to do.
        log_debug("{} stopped", inst)
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
            log_err("failed to stop {} (pg_ctl exited with {})", inst, rc)
            return OCF_ERR_GENERIC
        # Double check that the instance is stopped correctly.
        rc = ocf_monitor()
        if rc != OCF_NOT_RUNNING:
            log_err("unexpected {} state: monitor status "
                    "({}) disagree with pg_ctl return code", inst, rc)
            return OCF_ERR_GENERIC
    # At this point, the instance **MUST** be stopped gracefully.
    # What we are left to do here is to start again which will do the actual
    # demotion by mean of setting up the recovery.conf.
    rc = ocf_start()
    if rc == OCF_SUCCESS:
        log_info("{} started as a standby", inst)
        return OCF_SUCCESS
    log_err("failed to start {} as standby (returned {})", inst, rc)
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
        return notify_post_promote()
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
