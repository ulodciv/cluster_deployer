#!/usr/bin/python2.7
import os
import pprint
import datetime
import pwd
import re
import sys
import tempfile
from subprocess import call, check_output, CalledProcessError, STDOUT
from collections import defaultdict, namedtuple
from distutils.version import StrictVersion
from functools import partial
from itertools import izip_longest
from tempfile import gettempdir
from time import sleep

VERSION = "1.0"
PROGRAM = "pgsqlms2"
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
RE_TPL_TIMELINE = re.compile(
    r"^\s*recovery_target_timeline\s*=\s*'?latest'?\s*$", re.M)
RE_STANDBY_MODE = re.compile(r"^\s*standby_mode\s*=\s*'?on'?\s*$", re.M)
RE_APP_NAME = re.compile(
    r"^\s*primary_conninfo\s*=.*['\s]application_name=(?P<name>.*?)['\s]", re.M)
RE_PG_CLUSTER_STATE = re.compile(r"^Database cluster state:\s+(.*?)\s*$", re.M)
pguser_default = "postgres"
bindir_default = "/usr/bin"
pgdata_default = "/var/lib/pgsql/data"
pghost_default = "/var/run/postgresql"
pgport_default = "5432"
OCF_META_DATA = """\
<?xml version="1.0"?>
<!DOCTYPE resource-agent SYSTEM "ra-api-1.dtd">
<resource-agent name="pgsqlms2">
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


def env_else(var, else_val=None):
    try:
        v = os.environ[var]
        if v:
            return v
    except KeyError:
        pass
    return else_val


RSC_INST = env_else('OCF_RESOURCE_INSTANCE')
OCF_ACTION = sys.argv[1] if len(sys.argv) > 1 else None
LOG_TAG = "{}:{}({})[{}]".format(PROGRAM, OCF_ACTION, RSC_INST, os.getpid())


def hadate():
    return datetime.datetime.now().strftime(get_ha_datefmt())


def get_pguser():
    return env_else('OCF_RESKEY_pguser', pguser_default)


def get_bindir():
    return env_else('OCF_RESKEY_bindir', bindir_default)


def get_pgdata():
    return env_else('OCF_RESKEY_pgdata', pgdata_default)


def get_pghost():
    return env_else('OCF_RESKEY_pghost', pghost_default)


def get_pgport():
    return env_else('OCF_RESKEY_pgport', pgport_default)


def get_recovery_pcmk():
    return os.path.join(get_pgdata(), "recovery.conf.pcmk")


def get_postgresql_conf():
    return env_else(
        'OCF_RESKEY_pgconf', os.path.join(get_pgdata(), "postgresql.conf"))


def get_pgctl():
    return os.path.join(get_bindir(), "pg_ctl")


def get_psql():
    return os.path.join(get_bindir(), "psql")


def get_pgctrldata():
    return os.path.join(get_bindir(), "pg_controldata")


def get_pgisready():
    return os.path.join(get_bindir(), "pg_isready")


def get_ha_bin():
    return env_else('HA_SBIN_DIR', '/usr/sbin')


def get_ha_datefmt():
    return env_else('HA_DATEFMT', '%Y/%m/%d_%T ')


def get_ha_debuglog():
    return env_else('HA_DEBUGLOG', '')


def get_crm_master():
    return os.path.join(get_ha_bin(), "crm_master") + " --lifetime forever"


def get_crm_node():
    return os.path.join(get_ha_bin(), "crm_node")


def get_crm_failcount():
    return os.path.join(get_ha_bin(), "crm_failcount")


def get_pacemakerd():
    return os.path.join(get_ha_bin(), "pacemakerd")


def as_postgres_user():
    u = pwd.getpwnam(get_pguser())
    os.initgroups(get_pguser(), u.pw_gid)
    os.setgid(u.pw_gid)
    os.setuid(u.pw_uid)
    os.seteuid(u.pw_uid)


def ocf_log(level, msg, *args):
    l = level + ": " + msg.format(*args)
    if env_else('HA_LOGFACILITY', 'none') == 'none':
        os.environ['HA_LOGFACILITY'] = ''
    ha_logfacility = env_else('HA_LOGFACILITY', '')
    # if we're connected to a tty, then output to stderr
    if sys.stderr.isatty():
        sys.stderr.write("{}: {}\n".format(LOG_TAG, l))
        return 0
    if env_else('HA_LOGD', '') == 'yes':
        if call(['ha_logger', '-t', LOG_TAG, l]) == 0:
            return 0
    if ha_logfacility != '':
        # logging through syslog
        # loglevel is unknown, use 'notice' for now
        if level in ("ERROR", "CRIT"):
            loglevel = "err"
        elif level == "WARNING":
            loglevel = "warning"
        elif level == "INFO":
            loglevel = "info"
        elif level == "DEBUG":
            loglevel = "debug"
        else:
            loglevel = "notice"
        call(['logger', '-t', LOG_TAG, '-p', ha_logfacility + "." + loglevel, l])
    ha_logfile = env_else('HA_LOGFILE', '')
    if ha_logfile != '':
        with open(ha_logfile, "a") as f:
            f.write("{}: {} {}\n".format(LOG_TAG, hadate(), l))
    # appending to stderr
    if not ha_logfile and not ha_logfacility:
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


def get_notify_dict():
    notify_env = {
        'type': env_else('OCF_RESKEY_CRM_meta_notify_type', ''),
        'operation': env_else('OCF_RESKEY_CRM_meta_notify_operation', ''),
        'active': [], 'inactive': [], 'start': [], 'stop': [],
        'master': [], 'slave': [], 'promote': [], 'demote': []}
    actions = ['active', 'inactive', 'start', 'stop',
               'master', 'slave', 'promote', 'demote']
    for action in actions:
        rsc_key = "OCF_RESKEY_CRM_meta_notify_{}_resource".format(action)
        uname_key = "OCF_RESKEY_CRM_meta_notify_{}_uname".format(action)
        if not (rsc_key in os.environ and uname_key in os.environ):
            continue
        rscs = os.environ[rsc_key].split()
        unames = os.environ[uname_key].split()
        for rsc, uname in izip_longest(rscs, unames):
            d = {}
            if rsc is not None:
                d['rsc'] = rsc
            if uname is not None:
                d['uname'] = uname
            notify_env[action].append(d)
    return notify_env


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


def ocf_validate_all():
    # check binaries
    if not all(os.access(f, os.X_OK) for f in [
            get_pgctl(), get_psql(), get_pgisready(), get_pgctrldata()]):
        sys.exit(OCF_ERR_INSTALLED)
    if not os.path.isdir(get_pgdata()):
        log_err('PGDATA "{}" not found'.format(get_pgdata()))
        sys.exit(OCF_ERR_ARGS)
    datadir = get_pgdata()
    if not os.path.isdir(datadir):
        log_err('data_directory "{}" not found'.format(datadir))
        sys.exit(OCF_ERR_ARGS)
    # check PG_VERSION
    f = os.path.join(datadir, "PG_VERSION")
    if not os.path.isfile(f) or os.stat(f).st_size <= 0:
        log_crit('PG_VERSION not found in "{}"'.format(datadir))
        sys.exit(OCF_ERR_ARGS)
    # check recovery template
    recovery_tpl = get_recovery_pcmk()
    if not os.path.isfile(recovery_tpl):
        log_crit('Recovery template "{}" not found'.format(recovery_tpl))
        sys.exit(OCF_ERR_ARGS)
    # check content of the recovery template file
    try:
        with open(recovery_tpl) as f:
            content = f.read()
    except:
        log_crit('Could not open or read file "{}"'.format(recovery_tpl))
        sys.exit(OCF_ERR_ARGS)
    if not RE_STANDBY_MODE.search(content):
        log_crit(
            'Recovery template file {} must contain "standby_mode = on"',
            recovery_tpl)
        sys.exit(OCF_ERR_ARGS)
    if not RE_TPL_TIMELINE.search(content):
        log_crit(
            "Recovery template lacks \"recovery_target_timeline = 'latest'\"")
        sys.exit(OCF_ERR_ARGS)
    m = RE_APP_NAME.findall(content)
    if not m or not m[0].startswith(get_ocf_nodename()):
        log_crit('Recovery template missing parameter "application_name={}" in '
                 'primary_conninfo', get_ocf_nodename())
        sys.exit(OCF_ERR_ARGS)
    # check system user
    try:
        pwd.getpwnam(get_pguser())
    except KeyError:
        log_crit('System user "{}" does not exist', get_pguser())
        sys.exit(OCF_ERR_ARGS)
    # require 9.3 minimum
    try:
        with open(os.path.join(datadir, "PG_VERSION")) as f:
            ver = f.read()
    except:
        log_crit("Can't open {}", os.path.join(datadir, "PG_VERSION"))
        sys.exit(OCF_ERR_ARGS)
    ver = StrictVersion(ver.rstrip("\n"))
    if ver < StrictVersion('9.3'):
        log_err("PostgreSQL {} is too old: >= 9.3 required", ver)
        sys.exit(OCF_ERR_INSTALLED)
    # require wal_level >= hot_standby
    # NOTE: pg_controldata output changed with PostgreSQL 9.5, so we need to
    # account for both syntaxes
    p = re.compile(r"^(?:Current )?wal_level setting:\s+(.*?)\s*$", re.M)
    finds = p.findall(run_pgctrldata())
    if not finds:
        log_crit('Could not read wal_level setting')
        sys.exit(OCF_ERR_ARGS)
    if finds[0] not in ('hot_standby', 'logical', 'replica'):
        log_crit('wal_level must be hot_standby, logical or replica')
        sys.exit(OCF_ERR_ARGS)
    return OCF_SUCCESS


# returns true if the CRM is currently running a probe. A probe is
# defined as a monitor operation with a monitoring interval of zero.
def ocf_is_probe():
    return (OCF_ACTION == 'monitor' and
            env_else('OCF_RESKEY_CRM_meta_interval', "0") == "0")


# Returns 0 if instance is listening on the given host/port, otherwise:
#   1: ping rejected (usually when instance is in startup, in crash
#      recovery, in warm standby, or when a shutdown is in progress)
#   2: no response, usually means the instance is down
#   3: no attempt, probably a syntax error, should not happen
def run_pgisready():
    return as_postgres(
        [get_pgisready(), '-h', get_pghost(), '-p', get_pgport()])


# Run the given command as the "pg_user" by forking away from root
def as_postgres(cmd):
    cmd = [str(c) for c in cmd]
    log_debug('as_postgres: "{}" as {}', " ".join(cmd), get_pguser())
    with open(os.devnull, 'w') as DEVNULL:
        return call(cmd, cwd="/tmp", preexec_fn=as_postgres_user,
                    stdout=DEVNULL, stderr=STDOUT)


# Run a query using psql.
def pg_execute(query):
    connstr = "dbname=postgres"
    RS = chr(30)  # RS (record separator)
    FS = chr(3)  # ETX (end of text)
    tmp_fh, tmp_filename = tempfile.mkstemp(prefix='pgsqlms-')
    # if there is an error to catch, do:
    # ocf_log( 'crit', 'pg_execute: could not create or write in a temp file');
    # exit $OCF_ERR_INSTALLED;
    os.write(tmp_fh, query)
    os.close(tmp_fh)
    os.chmod(tmp_filename, 0o644)
    try:
        cmd = [
            get_psql(), '-v', 'ON_ERROR_STOP=1', '-qXAtf', tmp_filename, '-R',
            RS, '-F', FS, '-p', get_pgport(), '-h', get_pghost(), connstr]
        log_debug('pg_execute: running {}', " ".join(cmd))
        ans = check_output(cmd, cwd="/tmp", preexec_fn=as_postgres_user)
    except CalledProcessError as e:
        log_debug('pg_execute: psql error, return code: {}', e.returncode)
        # Possible return codes:
        #  -1: wrong parameters
        #   1: failed to get resources (memory, missing file, ...)
        #   2: unable to connect
        #   3: query failed
        return e.returncode, []
    finally:
        os.remove(tmp_filename)
    rs = []
    if ans:
        ans = ans[:-1]
        for record in ans.split(RS):
            rs.append(record.split(FS))
        log_debug('pg_execute: rs: {}', rs)
    return 0, rs


def get_ha_nodes():
    cmd = [get_crm_node(), "-p"]
    try:
        return check_output(cmd).split()
    except CalledProcessError as e:
        log_err(
            'get_ha_nodes: {} failed with return code {}', cmd, e.returncode)
        sys.exit(OCF_ERR_GENERIC)


def get_connected_standbies():
    # We check locations of connected standbies by querying the
    # "pg_stat_replication" view.
    # The row_number applies on the result set ordered on write_location ASC so
    # the highest row_number should be given to the closest node from the
    # master, then the lowest node name (alphanumeric sort) in case of equality.
    # The result set itself is order by priority DESC to process best known
    # candidate first.
    query = """
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
      ORDER BY priority DESC
    """
    rc, rs = pg_execute(query)
    if rc != 0:
        log_err(
            'get_connected_standbies: query to get standby locations failed ({})',
            rc)
        sys.exit(OCF_ERR_GENERIC)
    if not rs:
        log_warn('get_connected_standbies: No secondary connected')
    nt = namedtuple('nt', 'application_name priority location state')
    return [nt(*r) for r in rs]


# Check the write_location of secondaries, and adapt their master score so
# that the instance closest to the master will be the selected candidate should
# a promotion be triggered.
# NOTE: This is only a hint to pacemaker! The selected candidate to promotion
# actually re-checks that it is the best candidate and forces a re-election by
# failing if a better one exists.
# This avoid a race condition between the call of the monitor action and the
# promotion where another slave might have catchup faster with the master.
# NOTE: we cannot directly use write_location, nor lsn_diff as promotion score
# because Pacemaker considers any value greater than 1,000,000 to be INFINITY.
#
# Supposed to be called from a master monitor action.
def _check_locations():
    nodename = get_ocf_nodename()
    # Call crm_node to exclude nodes that are not part of the cluster at this
    # point.
    partition_nodes = get_ha_nodes()
    # For each standby connected, set their master score based on the following
    # rule: the first known node/application, with the highest priority and
    # with an acceptable state.
    for standby in get_connected_standbies():
        if standby.application_name not in partition_nodes:
            log_info(
                '_check_locations: ignoring unknown application_name/node "{}"',
                standby.application_name)
            continue
        if standby.application_name == nodename:
            log_warn('_check_locations: streaming replication with myself!')
            continue
        node_score = get_master_score(standby.application_name)
        if standby.state.strip() in ("startup", "backup"):
            # We exclude any standby being in state backup (pg_basebackup) or
            # startup (new standby or failing standby)
            log_info('_check_locations: forbid promotion on "{}" in state {}, '
                     'set score to -1', standby.application_name, standby.state)
            if node_score != '-1':
                set_master_score('-1', standby.application_name)
        else:
            log_debug(
                '_check_locations: checking "{}" promotion ability '
                '(current_score: {}, priority: {}, location: {}).',
                standby.application_name, node_score,
                standby.priority, standby.location)
            if node_score != standby.priority:
                log_info('_check_locations: update score of "{}" from {} to {}',
                         standby.application_name, node_score, standby.priority)
                set_master_score(standby.priority, standby.application_name)
            else:
                log_debug(
                    '_check_locations: "{}" keeps its current score of {}',
                    standby.application_name, standby.priority)
        # Remove this node from the known nodes list.
        partition_nodes.remove(standby.application_name.strip())
    # If there are still nodes in "partition_nodes", it means there is no
    # corresponding line in "pg_stat_replication".
    for node in partition_nodes:
        # Exclude the current node.
        if node == nodename:
            continue
        log_warn('_check_locations: "{}" is not connected to the primary, '
                 'set score to -1000', node)
        set_master_score('-1000', node)
    # Finally set the master score if not already done
    node_score = get_master_score()
    if node_score != "1001":
        set_master_score('1001')
    return OCF_SUCCESS


# Check to confirm if the instance is really started as run_pgisready stated and
# check if the instance is primary or secondary.
def _confirm_role():
    rc, rs = pg_execute("SELECT pg_is_in_recovery()")
    is_in_recovery = rs[0][0]
    if rc == 0:
        # The query was executed, check the result.
        if is_in_recovery == 't':
            # The instance is a secondary.
            log_debug("_confirm_role: instance {} is a secondary", RSC_INST)
            return OCF_SUCCESS
        elif is_in_recovery == 'f':
            # The instance is a primary.
            log_debug("_confirm_role: instance {} is a primary", RSC_INST)
            # Check lsn diff with current slaves if any
            if OCF_ACTION == 'monitor':
                _check_locations()
            return OCF_RUNNING_MASTER
        # This should not happen, raise a hard configuration error.
        log_err('_confirm_role: unexpected result from query to check if "{}" '
                'is a primary or a secondary: "{}"', RSC_INST, is_in_recovery)
        return OCF_ERR_CONFIGURED
    elif rc in (1, 2):
        # psql cound not connect to the instance.
        # As pg_isready reported the instance was listening, this error
        # could be a max_connection saturation. Just report a soft error.
        log_err('_confirm_role: psql can\'t connect to "{}"', RSC_INST)
        return OCF_ERR_GENERIC
    # The query failed (rc: 3) or bad parameters (rc: -1).
    # This should not happen, raise a hard configuration error.
    log_err('_confirm_role: the query to check if instance "{}" is a primary '
            'or a secondary failed (rc: {})', RSC_INST, rc)
    return OCF_ERR_CONFIGURED


# Parse and return the current status of the local PostgreSQL instance as
# reported by its controldata file
# WARNING: the status is NOT updated in case of crash.
def get_pg_cluster_state():
    datadir = get_pgdata()
    finds = RE_PG_CLUSTER_STATE.findall(run_pgctrldata())
    if not finds:
        log_crit('get_pg_cluster_state: could not read state from controldata '
                 'file for "{}"', datadir)
        sys.exit(OCF_ERR_CONFIGURED)
    log_debug('get_pg_cluster_state: state of {} is "{}"', RSC_INST, finds[0])
    return finds[0]


# Loop until pg_controldata returns a non-transitional state
# When this happens: return an OCF status based on the cluster state.
# Used to find out if this instance is a primary or secondary.
# Also used to detect if the instance has crashed.
def get_ocf_status():
    while True:
        state = get_pg_cluster_state()
        if state == '':
            # Something went really wrong with pg_controldata.
            log_err("get_ocf_status: no PG cluster state for {}", RSC_INST)
            sys.exit(OCF_ERR_INSTALLED)
        # Instance should be running as a primary.
        if state == "in production":
            return OCF_RUNNING_MASTER
        # Instance should be running as a secondary.
        # This state includes warm standby (rejects connections attempts,
        # including pg_isready)
        if state == "in archive recovery":
            return OCF_SUCCESS
        # The instance should be stopped.
        # We don't care if it was a primary or secondary before, because we
        # always start instances as secondaries, and then promote if necessary.
        if state in ("shut down", "shut down in recovery"):
            return OCF_NOT_RUNNING
        # The state is "in crash recovery", "starting up" or "shutting down".
        # This state should be transitional, so we wait and loop to check if
        # it changes.
        # If it does not, pacemaker will eventually abort with a timeout.
        log_debug('get_ocf_status: waiting for '
                  'transitionnal state "{}" to finish', state)
        sleep(1)


# Check the postmaster.pid file and the postmaster process.
# WARNING: doesn't distinguish a missing postmaster.pid from a process that is
# still alive. This is fine: monitor will find this to be a hard error.
def get_pg_ctl_status():
    rc = as_postgres([get_pgctl(), 'status', '-D', get_pgdata()])
    # pg_ctl status exits with 3 when postmaster.pid does not exist or the process
    # with the PID is not alive (otherwise it returns 0)s
    return rc


# Confirm PG is really stopped as pgisready stated
# and that it was propertly shut down.
def confirm_stopped():
    # Check the postmaster process status.
    pgctlstatus_rc = get_pg_ctl_status()
    if pgctlstatus_rc == 0:
        # The PID file exists and the process is available.
        # That should not be the case, return an error.
        log_err('confirm_stopped: instance "{}" is not listening, but the '
                'process referenced in postmaster.pid exists', RSC_INST)
        return OCF_ERR_GENERIC
    # The PID file does not exist or the process is not available.
    log_debug('confirm_stopped: no postmaster process found for instance "{}"',
              RSC_INST)
    if os.path.isfile(get_pgdata() + "/backup_label"):
        # We are probably on a freshly built secondary that was not started yet.
        log_debug('confirm_stopped: backup_label file exists: probably '
                  'on a never started secondary')
        return OCF_NOT_RUNNING
    # Continue the check with pg_controldata.
    controldata_rc = get_ocf_status()
    if controldata_rc == OCF_RUNNING_MASTER:
        # The controldata has not been updated to "shutdown".
        # It should mean we had a crash on a primary instance.
        log_err(
            'confirm_stopped: instance "{}" controldata indicates a running '
            'primary instance, the instance has probably crashed', RSC_INST)
        return OCF_FAILED_MASTER
    elif controldata_rc == OCF_SUCCESS:
        # The controldata has not been updated to "shutdown in recovery".
        # It should mean we had a crash on a secondary instance.
        # There is no "FAILED_SLAVE" return code, so we return a generic error.
        log_err('confirm_stopped: "{}" appears to be a running secondary, the '
                'instance has probably crashed', RSC_INST)
        return OCF_ERR_GENERIC
    elif controldata_rc == OCF_NOT_RUNNING:
        # The controldata state is consistent, the instance was probably
        # propertly shut down.
        log_debug('confirm_stopped: instance "{}" controldata indicates '
                  'that the instance was propertly shut down', RSC_INST)
        return OCF_NOT_RUNNING
    # Something went wrong with the controldata check.
    log_err('confirm_stopped: could not get instance "{}" status from '
            'controldata (returned: {})', RSC_INST, controldata_rc)
    return OCF_ERR_GENERIC


# Monitor the PostgreSQL instance
def ocf_monitor():
    if ocf_is_probe():
        log_debug("ocf_monitor: monitor is a probe")
    # First check, verify if the instance is listening.
    pgisready_rc = run_pgisready()
    if pgisready_rc == 0:
        # The instance is listening.
        # The instance is up and return if it is a primary or a secondary
        log_debug('ocf_monitor: instance "{}" is listening', RSC_INST)
        return _confirm_role()
    if pgisready_rc == 1:
        # The attempt was rejected.
        # This could happen in several cases:
        #   - at startup
        #   - during shutdown
        #   - during crash recovery
        #   - if instance is a warm standby
        # Except for the warm standby case, this should be a transitional state.
        # We try to confirm using pg_controldata.
        log_debug('ocf_monitor: instance "{}" rejects connections - '
                  'checking again...', RSC_INST)
        controldata_rc = get_ocf_status()
        if controldata_rc in (OCF_RUNNING_MASTER, OCF_SUCCESS):
            # This state indicates that pg_isready check should succeed.
            # We check again.
            log_debug('ocf_monitor: instance "{}" controldata shows a '
                      'running status', RSC_INST)
            pgisready_rc = run_pgisready()
            if pgisready_rc == 0:
                # Consistent with pg_controdata output.
                # We can check if the instance is primary or secondary
                log_debug('ocf_monitor: instance "{}" is listening', RSC_INST)
                return _confirm_role()
            # Still not consistent, raise an error.
            # NOTE: if the instance is a warm standby, we end here.
            # TODO raise an hard error here ?
            log_err('ocf_monitor: controldata of "{}"  is not consistent '
                    'with pg_isready (returned: {})', RSC_INST, pgisready_rc)
            log_info('ocf_monitor: if this instance is in warm standby, '
                     'this resource agent only supports hot standby')
            return OCF_ERR_GENERIC
        if controldata_rc == OCF_NOT_RUNNING:
            # This state indicates that pg_isready check should fail with rc 2.
            # We check again.
            pgisready_rc = run_pgisready()
            if pgisready_rc == 2:
                # Consistent with pg_controdata output.
                # We check the process status using pg_ctl status and check
                # if it was propertly shut down using pg_controldata.
                log_debug('ocf_monitor: "{}" is not listening', RSC_INST)
                return confirm_stopped()
            # Still not consistent, raise an error.
            # TODO raise an hard error here ?
            log_err('ocf_monitor: controldata of "{}" is not consistent '
                    'with pg_isready (returned: {})', RSC_INST, pgisready_rc)
            return OCF_ERR_GENERIC
        # Something went wrong with the controldata check, hard fail.
        log_err('ocf_monitor: could not get instance "{}" status from '
                'controldata (returned: {})', RSC_INST, controldata_rc)
        return OCF_ERR_INSTALLED
    elif pgisready_rc == 2:
        # The instance is not listening.
        # We check the process status using pg_ctl status and check
        # if it was propertly shut down using pg_controldata.
        log_debug('ocf_monitor: instance "{}" is not listening', RSC_INST)
        return confirm_stopped()
    elif pgisready_rc == 3:
        # No attempt was done, probably a syntax error.
        # Hard configuration error, we don't want to retry or failover here.
        log_err('ocf_monitor: unknown error while checking if "{}" '
                'is listening (returned {})', RSC_INST, pgisready_rc)
        return OCF_ERR_CONFIGURED
    log_err('ocf_monitor: unexpected pg_isready status for "{}"', RSC_INST)
    return OCF_ERR_GENERIC


# Create the recovery file based on the given template.
# Given template MUST at least contain:
#   standby_mode=on
#   primary_conninfo='...'
#   recovery_target_timeline = 'latest'
def _create_recovery_conf():
    u = pwd.getpwnam(get_pguser())
    uid, gid = u.pw_uid, u.pw_gid
    recovery_file = os.path.join(get_pgdata(), "recovery.conf")
    recovery_tpl = get_recovery_pcmk()
    log_debug('_create_recovery_conf: get replication configuration from '
              'the template file "{}"', recovery_tpl)
    # Create the recovery.conf file to start the instance as a secondary.
    # NOTE: the recovery.conf is supposed to be set up so the secondary can
    # connect to the primary instance, usually using a virtual IP address.
    # As there is no primary instance available at startup, secondaries will
    # complain about failing to connect.
    # As we can not reload a recovery.conf file on a standby without restarting
    # it, we will leave with this.
    # FIXME how would the reload help us in this case ?
    try:
        with open(recovery_tpl) as fh:
            # Copy all parameters from the template file
            recovery_conf = fh.read()
    except:
        log_crit("_create_recovery_conf: can't open {}", recovery_tpl)
        sys.exit(OCF_ERR_CONFIGURED)
    log_debug('_create_recovery_conf: writing "{}"', recovery_file)
    try:
        # Write recovery.conf using configuration from the template file
        with open(recovery_file, "w") as fh:
            fh.write(recovery_conf)
    except:
        log_crit("_create_recovery_conf: can't open {}", recovery_file)
        sys.exit(OCF_ERR_CONFIGURED)
    try:
        os.chown(recovery_file, uid, gid)
    except:
        log_crit("_create_recovery_conf: can't set owner of {}", recovery_file)
        sys.exit(OCF_ERR_CONFIGURED)


# Start the local instance using pg_ctl
def pg_ctl_start():
    # insanely long timeout to ensure Pacemaker gives up first
    cmd = [get_pgctl(), 'start', '-D', get_pgdata(), '-w', '-t', 1000000,
           '-o', "-c config_file=" + get_postgresql_conf()]
    return as_postgres(cmd)


def pg_ctl_stop():
    return as_postgres([get_pgctl(), 'stop', '-D', get_pgdata(),
                        '-w', '-t', 1000000, '-m', 'fast'])


# Get the resource master score of a node
def get_master_score(node=None):
    cmd = [get_crm_master(), "--quiet", "--get-value"]
    if node:
        cmd.extend(["-N", node])
    try:
        score = check_output(" ".join(cmd), shell=True)
    except CalledProcessError:
        return ''
    return score.strip()


# Check if a master score is set for one of the relative clones
# in the cluster and the score is greater or equal of 0.
# Returns True if at least one master score >= 0 is found, False otherwise
def _master_score_exists():
    partition_nodes = get_ha_nodes()
    for node in partition_nodes:
        score = get_master_score(node)
        if score != "" and int(score) > -1:
            return True
    return False


# Set the given attribute name to the given value.
# As setting an attribute is asynchronous, this will return as soon as the
# attribute is really set by attrd and available everywhere.
def set_master_score(score, node=None):
    cmd = [get_crm_master(), "-q", "-v", score]
    if node:
        cmd.extend(["-N", node])
    call(" ".join(cmd), shell=True)
    while True:
        tmp = get_master_score(node)
        if tmp == score:
            break
        log_debug('set_master_score: waiting to set score to {} '
                  '(currently {})...', score, tmp)
        sleep(0.1)


# Start the PostgreSQL instance as a *secondary*
def ocf_start():
    rc = ocf_monitor()
    prev_state = get_pg_cluster_state()
    # Instance must be running as secondary or being stopped.
    # Anything else is an error.
    if rc == OCF_SUCCESS:
        log_info('ocf_start: {} is already started', RSC_INST)
        return OCF_SUCCESS
    elif rc != OCF_NOT_RUNNING:
        log_err('ocf_start: unexpected state for {} (returned {})', RSC_INST, rc)
        return OCF_ERR_GENERIC
    #
    # From here, the instance is NOT running for sure.
    #
    log_debug('ocf_start: {} is stopped, starting it as a secondary', RSC_INST)
    # Create recovery.conf from the template file.
    _create_recovery_conf()
    # Start the instance as a secondary.
    rc = pg_ctl_start()
    if rc == 0:
        # Wait for the start to finish.
        while True:
            rc = ocf_monitor()
            if rc != OCF_NOT_RUNNING:
                break
            sleep(1)
        if rc == OCF_SUCCESS:
            log_info('ocf_start: {} started', RSC_INST)
            # Check if a master score exists in the cluster.
            # During the first cluster start, no master score will exist on any
            # of the slaves, unless an admin designated one using crm_master.
            # If no master exists the cluster won't promote one from the slaves.
            # To solve this, we check if there is at least one master
            # score existing on one node. Do nothing if at least one master
            # score is found in the clones of the resource. If no master score
            # exists, set a score of 1 only if the resource was a
            # master shut down before the start.
            if prev_state == "shut down" and not _master_score_exists():
                log_info('ocf_start: no master score around; set mine to 1')
                set_master_score('1')
            return OCF_SUCCESS
        log_err('ocf_start: {} is not running as a slave (returned {})',
                RSC_INST, rc)
        return OCF_ERR_GENERIC
    log_err('ocf_start: {} failed to start (rc: {})', RSC_INST, rc)
    return OCF_ERR_GENERIC


# Stop the PostgreSQL instance
def ocf_stop():
    # Instance must be running as secondary or primary or being stopped.
    # Anything else is an error.
    rc = ocf_monitor()
    if rc == OCF_NOT_RUNNING:
        log_info('ocf_stop: {} already stopped', RSC_INST)
        return OCF_SUCCESS
    elif rc not in (OCF_SUCCESS, OCF_RUNNING_MASTER):
        log_warn('ocf_stop: unexpected state for {} (returned {})', RSC_INST, rc)
        return OCF_ERR_GENERIC
    # From here, the instance is running for sure.
    log_debug('ocf_stop: {} is running, stopping it', RSC_INST)
    # Try to quit with proper shutdown.
    # insanely long timeout to ensure Pacemaker gives up first
    if pg_ctl_stop() == 0:
        log_info('ocf_stop: {} stopped', RSC_INST)
        return OCF_SUCCESS
    log_err('ocf_stop: {} failed to stop', RSC_INST)
    return OCF_ERR_GENERIC


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


# Promote the secondary instance to primary
def ocf_promote():
    nodename = get_ocf_nodename()
    rc = ocf_monitor()
    if rc == OCF_SUCCESS:
        # Running as slave. Normal, expected behavior.
        log_debug('ocf_promote: {} running as a standby', RSC_INST)
    elif rc == OCF_RUNNING_MASTER:
        # Already a master. Unexpected, but not a problem.
        log_info('ocf_promote: {} running as a primary', RSC_INST)
        return OCF_SUCCESS
    elif rc == OCF_NOT_RUNNING:  # INFO this is not supposed to happen.
        # Currently not running. Need to start before promoting.
        log_info('ocf_promote: {} stopped, starting it', RSC_INST)
        rc = ocf_start()
        if rc != OCF_SUCCESS:
            log_err('ocf_promote: failed to start {}', RSC_INST)
            return OCF_ERR_GENERIC
    else:
        log_info('ocf_promote: unexpected error, cannot promote {}', RSC_INST)
        return OCF_ERR_GENERIC
    #
    # At this point, the instance **MUST** be started as a secondary.
    #
    # Cancel the switchover if it has been considered unsafe during pre-promote
    if get_ha_private_attr('cancel_switchover') == '1':
        log_err('ocf_promote: switch from pre-promote action canceled ')
        del_ha_private_attr('cancel_switchover')
        return OCF_ERR_GENERIC
    # Do not check for a better candidate if we try to recover the master
    # Recovery of a master is detected during pre-promote. It sets the
    # private attribute 'recover_master' to '1' if this is a master recovery.
    if get_ha_private_attr('recover_master') == '1':
        log_info('ocf_promote: recovering old master, no election needed')
    else:
        # The promotion is occurring on the best known candidate (highest
        # master score), as chosen by pacemaker during the last working monitor
        # on previous master (see ocf_monitor/_check_locations subs).
        # To avoid race conditions between the last monitor action on the
        # previous master and the *real* most up-to-date standby, we set each
        # standby location during pre-promote, and store them using the
        # "lsn_location" resource attribute.
        #
        # The best standby to promote has the highest LSN. If the
        # current resource is not the best one, we need to modify the master
        # scores accordingly, and abort the current promotion.
        log_debug('ocf_promote: checking if current node is the best '
                  'candidate for promotion')
        # Exclude nodes that are known to be unavailable (not in the current
        # partition) using the "crm_node" command
        active_nodes = get_ha_private_attr('nodes').split()
        node_to_promote = ''
        # Get the lsn_location attribute value of the current node, as set
        # during the "pre-promote" action.
        # It should be the greatest among the secondary instances.
        max_lsn = get_ha_private_attr('lsn_location')
        if max_lsn == '':
            # Should not happen since the "lsn_location" attribute should have
            # been updated during pre-promote.
            log_crit('ocf_promote: can not get current node LSN location')
            return OCF_ERR_GENERIC
        # convert location to decimal
        max_lsn = max_lsn.strip("\n")
        wal_num, wal_off = max_lsn.split('/')
        max_lsn_dec = (294967296 * int(wal_num, 16)) + int(wal_off, 16)
        log_debug('ocf_promote: current node lsn location: {}({})',
                  max_lsn, max_lsn_dec)
        # Now we compare with the other available nodes.
        for node in active_nodes:
            # We exclude the current node from the check.
            if node == nodename:
                continue
            # Get the "lsn_location" attribute value for the node, as set during
            # the "pre-promote" action.
            node_lsn = get_ha_private_attr('lsn_location', node)
            if node_lsn == '':
                # This should not happen as the "lsn_location" attribute should
                # have been updated during the "pre-promote" action.
                log_crit('ocf_promote: can not get LSN location of {}', node)
                return OCF_ERR_GENERIC
            # convert location to decimal
            node_lsn = node_lsn.strip('\n')
            wal_num, wal_off = node_lsn.split('/')
            node_lsn_dec = (4294967296 * int(wal_num, 16)) + int(wal_off, 16)
            log_debug('ocf_promote: comparing with {}: lsn is {}({})',
                      node, node_lsn, node_lsn_dec)
            # If the node has a bigger delta, select it as a best candidate to
            # promotion.
            if node_lsn_dec > max_lsn_dec:
                node_to_promote = node
                max_lsn_dec = node_lsn_dec
                # max_lsn = node_lsn
                log_debug('ocf_promote: found {} as a better candidate to '
                          'promote', node)
        # If any node has been selected, we adapt the master scores accordingly
        # and break the current promotion.
        if node_to_promote != '':
            log_info('ocf_promote: {} is the best candidate to promote, '
                     'aborting current promotion', node_to_promote)
            # Reset current node master score.
            set_master_score('1')
            # Set promotion candidate master score.
            set_master_score('1000', node_to_promote)
            # We fail the promotion to trigger another promotion transition
            # with the new scores.
            return OCF_ERR_GENERIC
            # Else, we will keep on promoting the current node.
    if as_postgres([get_pgctl(), 'promote', '-D', get_pgdata(), '-w', ]) != 0:
        # Promote the instance on the current node.
        log_err('ocf_promote: error during promotion')
        return OCF_ERR_GENERIC
    # promote is asynchronous: wait for it to finish
    while ocf_monitor() != OCF_RUNNING_MASTER:
        log_debug('ocf_promote: waiting for promote to complete')
        sleep(1)
    log_info('ocf_promote: promote complete')
    return OCF_SUCCESS


# Demote the PostgreSQL instance from primary to secondary
# To demote a PostgreSQL instance, we must:
#   * stop it gracefully
#   * create recovery.conf with standby_mode = on
#   * start it back
def ocf_demote():
    rc = ocf_monitor()
    # Running as primary. Normal, expected behavior.
    if rc == OCF_RUNNING_MASTER:
        log_debug('ocf_demote: {} running as a primary', RSC_INST)
    elif rc == OCF_SUCCESS:
        # Already running as secondary. Nothing to do.
        log_debug('ocf_demote: {} running as a secondary', RSC_INST)
        return OCF_SUCCESS
    elif rc == OCF_NOT_RUNNING:
        # Instance is stopped. Nothing to do.
        log_debug('ocf_demote: {} stopped', RSC_INST)
    elif rc == OCF_ERR_CONFIGURED:
        # We actually prefer raising a hard or fatal error instead of leaving
        # the CRM abording its transition for a new one because of a soft error.
        # The hard error will force the CRM to move the resource immediately.
        return OCF_ERR_CONFIGURED
    else:
        return OCF_ERR_GENERIC
    # TODO we need to make sure at least one slave is connected!!
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
            log_err('ocf_demote: failed to stop {} (pg_ctl exited with {})',
                    RSC_INST, rc)
            return OCF_ERR_GENERIC
        # Double check that the instance is stopped correctly.
        rc = ocf_monitor()
        if rc != OCF_NOT_RUNNING:
            log_err('ocf_demote: unexpected "{}" state: monitor status '
                    '({}) disagree with pg_ctl return code', RSC_INST, rc)
            return OCF_ERR_GENERIC
    #
    # At this point, the instance **MUST** be stopped gracefully.
    #
    # Note: We do not need to handle the recovery.conf file here as ocf_start
    # deal with that itself. Equally, no need to wait for the start to complete
    # here, handled in ocf_start.
    rc = ocf_start()
    if rc == OCF_SUCCESS:
        log_info('ocf_demote: {} started as a secondary', RSC_INST)
        return OCF_SUCCESS
    # NOTE: No need to double check the instance state as ocf_start already use
    # ocf_monitor to check the state before returning.
    log_err('ocf_demote: failed to start {} as standby (returned {})',
            RSC_INST, rc)
    return OCF_ERR_GENERIC


# Notify type actions, called on all available nodes before (pre) and after
# (post) other actions, like promote, start, ...
def ocf_notify():
    d = get_notify_dict()
    log_debug("ocf_notify: environment variables:\n{}", pprint.pformat(d))
    if not d:
        return
    type_op = d['type'] + "-" + d['operation']
    if type_op == "pre-promote":
        return notify_pre_promote()
    if type_op == "post-promote":
        return notify_post_promote()
    if type_op == "pre-demote":
        return notify_pre_demote()
    if type_op == "pre-stop":
        return notify_pre_stop()
    return OCF_SUCCESS


# Check if the current transiation is a recover of a master clone on given node.
def _is_master_recover(n):
    d = get_notify_dict()
    # n == d['promote'][0]['uname']
    return (any(m['uname'] == n for m in d['master']) and
            any(m['uname'] == n for m in d['promote']))


# Check if the current transition is a recover of a slave clone on given node.
def _is_slave_recover(n):
    d = get_notify_dict()
    return (any(m['uname'] == n for m in d['slave']) and
            any(m['uname'] == n for m in d['start']))


# check if th current transition is a switchover to the given node.
def _is_switchover(n):
    d = get_notify_dict()
    old = None
    if "master" in d and len(d['master']) > 0 and 'uname' in d['master'][0]:
        old = d['master'][0]['uname']
    if len(d['master']) != 1 or len(d['demote']) != 1 or len(d['promote']) != 1:
        return 0
    t1 = any(m['uname'] == old for m in d['demote'])
    t2 = any(m['uname'] == n for m in d['slave'])
    t3 = any(m['uname'] == n for m in d['promote'])
    t4 = any(m['uname'] == old for m in d['stop'])
    return t1 and t2 and t3 and not t4


# _check_switchover
# check if the pgsql switchover to the localnode is safe.
# This is supposed to be called **after** the master has been stopped or demote.
# This sub check if the local standby received the shutdown checkpoint from the
# old master to make sure it can take over the master role and the old master
# will be able to catchup as a standby after.
#
# Returns 0 if switchover is safe
# Returns 1 if swithcover is not safe
# Returns 2 for internal error
def _check_switchover():
    log_info('_check_switchover: switch from "{}" to "{}" in progress. '
             'Need to check the last record in WAL',
             get_notify_dict()['demote'][0]['uname'], get_ocf_nodename())
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
        log_err('_check_switchover: could not query '
                'last_xlog_receive_location ({})', rc)
        return 2
    last_lsn = rs[0][0] if rs and rs[0] else None
    if not (tl and last_chk and last_lsn):
        log_crit('_check_switchover: could not read last '
                 'checkpoint and timeline from controldata file!')
        log_debug('_check_switchover: XLOGDUMP parameters: '
                  'datadir: "{}", last_chk: {}, tl: {}, mast_lsn: {}',
                  datadir, last_chk, tl, last_lsn)
        return 2
    # force checkpoint on the slave to flush the master's
    # shutdown checkpoint to WAL
    rc = 0
    try:
        ans = check_output([
            os.path.join(get_bindir(), "pg_xlogdump"), "-p", datadir, "-t", tl,
            "-s", last_chk, "-e", last_lsn], stderr=STDOUT)
    except CalledProcessError as e:
        rc = e.returncode
        ans = e.output
    log_debug(
        '_check_switchover: XLOGDUMP rc: {}, tl: {}, last_chk: {}, '
        'last_lsn: {}, output: "{}"', rc, tl, last_chk, last_lsn, ans)
    p = re.compile(
        r"^rmgr: XLOG.*desc: [cC][hH][eE][cC][kK][pP][oO][iI][nN][tT]"
        r"(:|_SHUTDOWN) redo [0-9A-F/]+; tli " + tl + r";.*; shutdown$",
        re.M | re.DOTALL)
    if rc == 0 and p.search(ans):
        log_info(
            '_check_switchover: slave received the shutdown checkpoint',
            get_pg_cluster_state())
        return 0
    set_ha_private_attr('cancel_switchover', '1')
    log_info('ocf_notify: did not received the shutdown checkpoint from '
             'the old master!')
    return 1


def notify_pre_promote():
    node = get_ocf_nodename()
    d = get_notify_dict()
    promoting = d['promote'][0]['uname']
    log_info('ocf_notify: promoting instance on {}', promoting)
    # No need to do an election between slaves if this is recovery of the master
    if _is_master_recover(promoting):
        log_warn('ocf_notify: This is a master recovery!')
        if promoting == node:
            set_ha_private_attr('recover_master', '1')
        return OCF_SUCCESS
    # Environment cleanup!
    del_ha_private_attr('lsn_location')
    del_ha_private_attr('recover_master')
    del_ha_private_attr('nodes')
    del_ha_private_attr('cancel_switchover')
    # check for the last received entry of WAL from the master if we are
    # the designated slave to promote
    if _is_switchover(node) and any(m['uname'] == node for m in d['promote']):
        rc = _check_switchover()
        if rc == 1:
            # Shortcut the election process because the switchover will be
            # canceled
            return OCF_SUCCESS
        elif rc != 0:
            # This is an extreme mesure, it shouldn't happen.
            call([get_crm_failcount(), "-r", RSC_INST, "-v", "1000000"])
            return OCF_ERR_INSTALLED
            # If the sub keeps going, that means the switchover is safe.
            # Keep going with the election process in case the switchover was
            # instruct to the wrong node.
            # FIXME: should we allow a switchover to a lagging slave?
    # We need to trigger an election between existing slaves to promote the best
    # one based on its current LSN location. The designated standby for
    # promotion is responsible to connect to each available nodes to check their
    # "lsn_location".
    # During the following promote action, ocf_promote will use this
    # information to check if the instance to be promoted is the best one,
    # so we can avoid a race condition between the last successful monitor
    # on the previous master and the current promotion.
    rc, rs = pg_execute('SELECT pg_last_xlog_receive_location()')
    if rc != 0:
        log_warn('ocf_notify: could not query the current node LSN')
        # Return code are ignored during notifications...
        return OCF_SUCCESS
    node_lsn = rs[0][0]
    log_info('ocf_notify: current node LSN: {}', node_lsn)
    # Set the "lsn_location" attribute value for this node so we can use it
    # during the following "promote" action.
    if not set_ha_private_attr('lsn_location', node_lsn):
        log_warn('ocf_notify: could not set the current node LSN')
    # If this node is the future master, keep track of the slaves that
    # received the same notification to compare our LSN with them during
    # promotion
    active_nodes = defaultdict(int)
    if promoting == node:
        # build the list of active nodes:
        #   master + slave + start - stop
        for foo in d['master']:
            active_nodes[foo['uname']] += 1
        for foo in d['slave']:
            active_nodes[foo['uname']] += 1
        for foo in d['start']:
            active_nodes[foo['uname']] += 1
        for foo in d['stop']:
            active_nodes[foo['uname']] -= 1
        attr_nodes = " ".join(k for k in active_nodes if active_nodes[k] > 0)
        set_ha_private_attr('nodes', attr_nodes)
    return OCF_SUCCESS


def notify_post_promote():
    # We have a new master (or the previous one recovered).
    del_ha_private_attr('lsn_location')
    del_ha_private_attr('recover_master')
    del_ha_private_attr('nodes')
    del_ha_private_attr('cancel_switchover')
    return OCF_SUCCESS


def notify_pre_demote():
    # do nothing if the local node will not be demoted
    ocf_nodename = get_ocf_nodename()
    if all(m['uname'] != ocf_nodename for m in get_notify_dict()["demote"]):
        return OCF_SUCCESS
    rc = ocf_monitor()
    # do nothing if this is not a master recovery
    if not _is_master_recover(ocf_nodename) or rc != OCF_FAILED_MASTER:
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
    log_info('notify_pre_demote: trying to start failing master {}', RSC_INST)
    # Either the instance managed to start or it couldn't.
    # We rely on the pg_ctk '-w' switch to take care of this. If it couldn't
    # start, this error will be catched up later during the various checks
    pg_ctl_start()
    log_info('notify_pre_demote: state is "{}" after recovery attempt',
             get_pg_cluster_state())
    return OCF_SUCCESS


def notify_pre_stop():
    d = get_notify_dict()
    # do nothing if the local node will not be stopped
    if not any(m['uname'] == get_ocf_nodename() for m in d["stop"]):
        return OCF_SUCCESS
    rc = get_ocf_status()
    # do nothing if this is not a slave recovery
    if not _is_slave_recover(get_ocf_nodename()) or rc != OCF_RUNNING_SLAVE:
        return OCF_SUCCESS
    # in case of slave crash, we need to detect if the CRM tries to recover
    # the slaveclone. The usual transition is to do: stop->start
    #
    # This transition can not work because the instance is in
    # OCF_ERR_GENERIC step. So the stop action will fail, leading most
    # probably to fencing action.
    #
    # To avoid this, we try to start the instance in recovery from here.
    # If it succeeds, at least it will be stopped correctly with a normal
    # status. If it fails, it will be catched up in next steps.
    log_info('notify_pre_stop: trying to start failing slave {}', RSC_INST)
    # Either the instance managed to start or it couldn't.
    # We rely on the pg_ctl '-w' switch to take care of this. If it couldn't
    # start, this error will be catched up later during the various checks
    pg_ctl_start()
    log_info('notify_pre_stop: state is "{}" after recovery attempt',
             get_pg_cluster_state())
    return OCF_SUCCESS


if __name__ == "__main__":
    if OCF_ACTION == "meta-data":
        print(OCF_META_DATA)
        sys.exit()
    if OCF_ACTION == "methods":
        print(OCF_METHODS)
        sys.exit()
    os.chdir(gettempdir())
    if int(env_else('OCF_RESKEY_CRM_meta_clone_max', 0)) <= 0:
        log_err("OCF_RESKEY_CRM_meta_clone_max should be >= 1")
        sys.exit(OCF_ERR_CONFIGURED)
    if env_else('OCF_RESKEY_CRM_meta_master_max', "") != "1":
        log_err("OCF_RESKEY_CRM_meta_master_max should == 1")
        sys.exit(OCF_ERR_CONFIGURED)
    if OCF_ACTION == 'validate-all':
        sys.exit(ocf_validate_all())
    if OCF_ACTION not in (
            "start", "stop", "monitor", "promote", "demote", "notify"):
        sys.exit(OCF_ERR_UNIMPLEMENTED)
    ocf_validate_all()
    if OCF_ACTION == 'start':  # start as secondary only
        sys.exit(ocf_start())
    if OCF_ACTION == 'stop':
        sys.exit(ocf_stop())
    if OCF_ACTION == 'monitor':
        sys.exit(ocf_monitor())
    if OCF_ACTION == 'promote':
        sys.exit(ocf_promote())
    if OCF_ACTION == 'demote':
        sys.exit(ocf_demote())
    if OCF_ACTION == 'notify':
        sys.exit(ocf_notify())
