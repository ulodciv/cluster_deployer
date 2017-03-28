#!/usr/bin/python
import locale
import os
import pprint
import random
import datetime
import pwd
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from distutils.version import StrictVersion
from itertools import izip_longest, chain
from tempfile import gettempdir
from time import sleep


VERSION = "1.0"
PROGRAM = 'postgresql_ra'
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
nodename = None
exit_code = 0


def del_env(k):
    try:
        del os.environ[k]
    except KeyError:
        pass


def env_else(var, else_val=None):
    try:
        v = os.environ[var]
        if v:
            return v
    except KeyError:
        pass
    return else_val


def ha_log(*args):
    ignore_stderr = False
    loglevel = ''
    logtag = env_else('HA_LOGTAG', '')
    if args[0] == '--ignore-stderr':
        ignore_stderr = True
        args = args[1:]
    if env_else('HA_LOGFACILITY', 'none') == 'none':
        os.environ['HA_LOGFACILITY'] = ''
    ha_logfacility = env_else('HA_LOGFACILITY', '')
    # if we're connected to a tty, then output to stderr
    if sys.stderr.isatty():
        # FIXME
        # T.N.: this was ported with the bug on $loglevel being empty
        # and never set before the test here...
        if int(env_else('HA_debug', -1)) == 0 and loglevel == "debug":
            return 0
        elif ignore_stderr:
            # something already printed this error to stderr, so ignore
            return 0
        if logtag != '':
            sys.stderr.write("{}: {}\n".format(logtag, " ".join(args)))
        else:
            sys.stderr.write("{}\n".format(" ".join(args)))
        return 0
    set_logtag()
    if env_else('HA_LOGD', 'no') == 'yes':
        if subprocess.call(chain(['ha_logger', '-t', logtag], args)) == 0:
            return 0
    if ha_logfacility != '':
        # logging through syslog
        # loglevel is unknown, use 'notice' for now
        loglevel = 'notice'
        if any("ERROR" in a for a in args):
            loglevel = "err"
        elif any("WARN" in a for a in args):
            loglevel = "warning"
        elif any("INFO" in a or "info" in a for a in args):
            loglevel = "info"
        subprocess.call(chain(
            ['logger', '-t', logtag, '-p', ha_logfacility + "." + loglevel],
            args))
    ha_logfile = env_else('HA_LOGFILE', '')
    if ha_logfile != '':
        with open(ha_logfile, "a") as f:
            f.write("{}:	{} {}\n".format(logtag, hadate(), ' '.join(args)))
    # appending to stderr
    if not ha_logfile and not ignore_stderr and not ha_logfacility:
        sys.stderr.write("{} {}\n".format(hadate(), ' '.join(args)))
    ha_debuglog = env_else('HA_DEBUGLOG', '')
    if ha_debuglog and ha_debuglog != ha_logfile:
        with open(ha_debuglog, "a") as f:
            f.write("{}:	{} {}\n".format(logtag, hadate(), ' '.join(args)))


def ha_debug(*args):
    if int(env_else('HA_debug', -1)) == 0:
        return 0
    logtag = env_else('HA_LOGTAG', '')
    if sys.stderr.isatty():
        if logtag != '':
            sys.stderr.write("{}: {}\n".format(logtag, ' '.join(args)))
        else:
            sys.stderr.write("{}\n".format(' '.join(args)))
        return 0
    set_logtag()
    if env_else('HA_LOGD', 'no') == 'yes':
        if subprocess.call(chain(
                ['ha_logger', '-t', logtag, '-D', 'ha-debug'], args)) == 0:
            return 0
    if env_else('HA_LOGFACILITY', 'none') == 'none':
        os.environ['HA_LOGFACILITY'] = ''
    ha_logfacility = env_else('HA_LOGFACILITY', '')
    if ha_logfacility != '':
        # logging through syslog
        subprocess.call(chain(
            ['logger', '-t', logtag, '-p', ha_logfacility + ".debug"], args))
    ha_debuglog = env_else('HA_DEBUGLOG', '')
    if ha_debuglog and os.path.isfile(ha_debuglog):
        with open(ha_debuglog, "a") as f:
            f.write("{}:	{} {}\n".format(logtag, hadate(), ' '.join(args)))
    # appending to stderr
    if not ha_logfacility and not ha_debuglog and not ha_logfacility:
        sys.stderr.write("{}: {} {}\n".format(logtag, hadate(), ' '.join(args)))


def ocf_log_crit(msg, *args):
    ha_log("CRIT: " + msg.format(*args))


def ocf_log_err(msg, *args):
    ha_log("ERROR: " + msg.format(*args))


def ocf_log_warn(msg, *args):
    ha_log("WARNING: " + msg.format(*args))


def ocf_log_info(msg, *args):
    ha_log("INFO: " + msg.format(*args))


def ocf_log_debug(msg, *args):
    ha_debug("DEBUG: " + msg.format(*args))


# Parse and returns the notify environment variables in a convenient structure
# Returns undef if the action is not a notify
# Returns undef if the resource is neither a clone or a multistate one
def ocf_notify_env():
    if not (ocf_is_clone() or ocf_is_ms()):
        return None
    notify_env = {
        'type': env_else('OCF_RESKEY_CRM_meta_notify_type', ''),
        'operation': env_else('OCF_RESKEY_CRM_meta_notify_operation', ''),
        'active': [],
        'inactive': [],
        'start': [],
        'stop': []}
    actions = ['active', 'inactive', 'start', 'stop']
    if ocf_is_ms():  # exit if the resource is not a mutistate one
        actions.extend(['master', 'slave', 'promote', 'demote'])
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


def ocf_maybe_random():
    return random.randint(0, 32766)


def ocf_is_true(v):
    return v in (True, 'yes', 'true', 1, '1', 'YES', 'TRUE', 'ja', 'on', 'ON')


def ocf_is_clone():
    return int(env_else('OCF_RESKEY_CRM_meta_clone_max', 0)) > 0


def ocf_is_ms():
    return int(env_else('OCF_RESKEY_CRM_meta_master_max', 0)) > 0


def qx2(cmd):
    try:
        return 0, subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output


def qx(cmd):
    try:
        return subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        return e.output


def qx_check_only(cmd):
    return subprocess.call(cmd, shell=True) == 0


def ocf_local_nodename():
    p = re.compile(r"Pacemaker ([\d.]+)")
    # use crm_node -n for pacemaker > 1.1.8
    if qx_check_only(r"which pacemakerd > /dev/null 2>&1"):
        ret = qx(r"pacemakerd -\$")
        ret = p.findall(ret)[0]
        if StrictVersion(ret) > StrictVersion('1.1.8'):
            if qx_check_only(r"which crm_node > /dev/null 2>&1"):
                return qx("crm_node -n").strip()
    # otherwise use uname -n
    return qx("uname -n").strip()


def is_x_ok(f):
    return os.access(f, os.X_OK)


def file_is_not_empty(f):
    try:
        return os.stat(f).st_size > 0
    except OSError:
        return False


RE_TPL_TIMELINE = re.compile(r"^\s*recovery_target_timeline\s*=\s*'?latest'?\s*$", re.M)
RE_STANDBY_MODE = re.compile(r"^\s*standby_mode\s*=\s*'?on'?\s*$", re.M)
RE_APP_NAME = re.compile(r"^\s*primary_conninfo\s*=.*['\s]application_name=(?P<name>.*?)['\s]", re.M)


def pgsql_validate_all():
    # check binaries
    if not (is_x_ok(PGCTL) and is_x_ok(PGPSQL) and is_x_ok(PGCTRLDATA) and
            is_x_ok(PGISREADY)):
        sys.exit(OCF_ERR_INSTALLED)
    if not os.path.isdir(pgdata):
        ocf_log_err('PGDATA "{}" does not exists'.format(pgdata))
        sys.exit(OCF_ERR_ARGS)
    if not os.path.isdir(datadir):
        ocf_log_err('data_directory "{}" does not exists'.format(datadir))
        sys.exit(OCF_ERR_ARGS)
    # check PG_VERSION
    if not file_is_not_empty(os.path.join(datadir, "PG_VERSION")):
        ocf_log_crit('PG_VERSION does not exists in "{}"'.format(datadir))
        sys.exit(OCF_ERR_ARGS)
    # check recovery template
    if not os.path.isfile(recovery_tpl):
        ocf_log_crit('Recovery template file "{}" does not exist'.format(
            recovery_tpl))
        sys.exit(OCF_ERR_ARGS)
    # check content of the recovery template file
    try:
        with open(recovery_tpl) as f:
            content = f.read()
    except:
        ocf_log_crit('Could not open or read file "{}"'.format(recovery_tpl))
        sys.exit(OCF_ERR_ARGS)
    if not RE_STANDBY_MODE.search(content):
        ocf_log_crit(
            'Recovery template file {} must contain "standby_mode = on"',
            recovery_tpl)
        sys.exit(OCF_ERR_ARGS)
    if not RE_TPL_TIMELINE.search(content):
        ocf_log_crit("Recovery template file must contain "
                     "\"recovery_target_timeline = 'latest'\"")
        sys.exit(OCF_ERR_ARGS)
    m = RE_APP_NAME.findall(content)
    if not m or not m[0].startswith(nodename):
        ocf_log_crit(
            'Recovery template file must contain in primary_conninfo '
            'parameter "application_name={}"', nodename)
        sys.exit(OCF_ERR_ARGS)
    # check system user
    try:
        pwd.getpwnam(system_user)
    except KeyError:
        ocf_log_crit('System user "{}" does not exist', system_user)
        sys.exit(OCF_ERR_ARGS)

    # require 9.3 minimum
    try:
        with open(os.path.join(datadir, "PG_VERSION")) as f:
            ver = f.read()
    except:
        ocf_log_crit("Could not open file \"{}\"",
                     os.path.join(datadir, "PG_VERSION"))
        sys.exit(OCF_ERR_ARGS)
    ver = StrictVersion(ver.rstrip("\n"))
    if ver < StrictVersion('9.3'):
        ocf_log_err(
            "PostgreSQL version {} not supported. Require 9.3 and more.", ver)
        sys.exit(OCF_ERR_INSTALLED)
    # require wal_level >= hot_standby
    status = qx(PGCTRLDATA + " " + datadir + " 2>/dev/null")
    # NOTE: pg_controldata output changed with PostgreSQL 9.5, so we need to
    # account for both syntaxes
    p = re.compile(r"^(?:Current )?wal_level setting:\s+(.*?)\s*$", re.M)
    finds = p.findall(status)
    if not finds:
        ocf_log_crit('Could not read wal_level setting')
        sys.exit(OCF_ERR_ARGS)
    if finds[0] not in ('hot_standby', 'logical', 'replica'):
        ocf_log_crit(
            'wal_level must be one of "hot_standby", "logical" or "replica"')
        sys.exit(OCF_ERR_ARGS)
    return OCF_SUCCESS


# returns true if the CRM is currently running a probe. A probe is
# defined as a monitor operation with a monitoring interval of zero.
def ocf_is_probe():
    return (OCF_ACTION == 'monitor' and
            os.environ['OCF_RESKEY_CRM_meta_interval'] == "0")


# Check if instance is listening on the given host/port.
#
def _pg_isready():
    rc = _runas([PGISREADY, '-h', pghost, '-p', pgport])
    # Possible error codes:
    #   1: ping rejected (usually when instance is in startup, in crash
    #      recovery, in warm standby, or when a shutdown is in progress)
    #   2: no response, usually means the instance is down
    #   3: no attempt, probably a syntax error, should not happen
    return rc


# Run the given command as the "system_user" by forking away from root
def _runas(cmd):
    if os.fork() == 0:  # in child
        u = pwd.getpwnam(system_user)
        os.initgroups(system_user, u.pw_gid)
        os.setgid(u.pw_gid)
        os.setuid(u.pw_uid)
        os.seteuid(u.pw_uid)
        os.execv(cmd[0], cmd)
    ocf_log_debug(
        '_runas: launching as "{}" command "{}"', system_user, " ".join(cmd))
    pid, status = os.wait()
    return os.WEXITSTATUS(status)


# Run a query using psql.
#
# This function returns an array with psql return code as first element and
# the result as second one.
def _query(query):
    connstr = "dbname=postgres"
    RS = chr(30)  # ASCII RS  (record separator)
    FS = chr(3)  # ASCII ETX (end of text)
    tmp_fh, tmp_filename = tempfile.mkstemp(prefix='pgsqlms-')
    # if there is an error to catch, do:
    # ocf_log( 'crit', '_query: could not create or write in a temp file');
    # exit $OCF_ERR_INSTALLED;
    os.write(tmp_fh, query)
    os.write(tmp_fh, "\n")
    os.chmod(tmp_filename, 0o644)

    # Change the effective user to the given system_user so after forking
    # the given uid to the process should allow psql to connect w/o password
    def demote():
        os.seteuid(pwd.getpwnam(system_user).pw_uid)

    try:
        ans = subprocess.check_output(
            [PGPSQL, '--set', 'ON_ERROR_STOP=1', '-qXAtf', tmp_filename,
             '-R', RS, '-F', FS, '--port', pgport, '--host', pghost, connstr],
            preexec_fn=demote)
        rc = 0
    except OSError as e:
        rc = e.args[0]
        ans = None
    ocf_log_debug('_query: psql return code: {}', rc)
    rs = []
    if ans:
        ans = ans[:-1]
        for record in ans.split(RS):
            rs.append([record.split(FS)])
        ocf_log_debug('_query: @res: {}', rs)
    # Possible return codes:
    #  -1: wrong parameters
    #   0: OK
    #   1: failed to get resources (memory, missing file, ...)
    #   2: unable to connect
    #   3: query failed
    return rc, rs


# Check the write_location of all secondaries, and adapt their master score so
# that the instance closest to the master will be the selected candidate should
# a promotion be triggered.
# NOTE: This is only a hint to pacemaker! The selected candidate to promotion
# actually re-check it is the best candidate and force a re-election by failing
# if a better one exists. This avoid a race condition between the call of the
# monitor action and the promotion where another slave might have catchup faster
# with the master.
# NOTE: we cannot directly use the write_location, neither a lsn_diff value as
# promotion score as Pacemaker considers any value greater than 1,000,000 as
# INFINITY.
#
# This sub is supposed to be executed from a master monitor action.
def _check_locations():
    # Call crm_node to exclude nodes that are not part of the cluster at this
    # point.
    partition_nodes = qx(CRM_NODE + " --partition").split()
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
    rc, rs = _query(query)
    if rc != 0:
        ocf_log_err(
            '_check_locations: query to get standby locations failed ({})', rc)
        sys.exit(OCF_ERR_GENERIC)
    # If there is no row left at this point, it means that there is no
    # secondary instance connected.
    if not rs:
        ocf_log_warn('_check_locations: No secondary connected')
    # For each standby connected, set their master score based on the following
    # rule: the first known node/application, with the highest priority and
    # with an acceptable state.
    for row in rs:
        if row[0] not in partition_nodes:
            ocf_log_info(
                '_check_locations: ignoring unknown application_name/node "{}"',
                row[0])
            continue
        if row[0] == nodename:
            ocf_log_warn('_check_locations: streaming replication with myself!')
            continue
        node_score = _get_master_score(row[0])
        if row[3].strip() in ("startup", "backup"):
            # We exclude any standby being in state backup (pg_basebackup) or
            # startup (new standby or failing standby)
            ocf_log_info(
                '_check_locations: forbid promotion on "{}" in state "{}", '
                'set score to -1', row[0], row[3])
            if node_score != '-1':
                _set_master_score('-1', row[0])
        else:
            ocf_log_debug(
                '_check_locations: checking "{}" promotion ability '
                '(current_score: {}, priority: {}, location: {}).',
                row[0], node_score, row[1], row[2])
            if node_score != row[1]:
                ocf_log_info(
                    '_check_locations: update score of "{}" from {} to {}',
                    row[0], node_score, row[1])
                _set_master_score(row[1], row[0])
            else:
                ocf_log_debug(
                    '_check_locations: "{}" keeps its current score of {}',
                    row[0], row[1])
        # Remove this node from the known nodes list.
        partition_nodes.remove(row[0].strip())
    # If there are still nodes in "partition_nodes", it means there is no
    # corresponding line in "pg_stat_replication".
    for node in partition_nodes:
        # Exclude the current node.
        if node == nodename:
            continue
        ocf_log_warn('_check_locations: "{}" is not connected to the primary, '
                     'set score to -1000', node)
        _set_master_score('-1000', node)
    # Finally set the master score if not already done
    node_score = _get_master_score()
    if node_score != "1001":
        _set_master_score('1001')
    return OCF_SUCCESS


# Check to confirm if the instance is really started as _pg_isready stated and
# check if the instance is primary or secondary.
def _confirm_role():
    rc, rs = _query("SELECT pg_is_in_recovery()")
    is_in_recovery = rs[0][0]
    if rc == 0:
        # The query was executed, check the result.
        if is_in_recovery == 't':
            # The instance is a secondary.
            ocf_log_debug("_confirm_role: instance {} is a secondary",
                          OCF_RESOURCE_INSTANCE)
            return OCF_SUCCESS
        elif is_in_recovery == 'f':
            # The instance is a primary.
            ocf_log_debug("_confirm_role: instance {} is a primary",
                          OCF_RESOURCE_INSTANCE)
            # Check lsn diff with current slaves if any
            if OCF_ACTION == 'monitor':
                _check_locations()
            return OCF_RUNNING_MASTER
        # This should not happen, raise a hard configuration error.
        ocf_log_err('_confirm_role: unexpected result from query to check '
                    'if "{}" is a primary or a secondary: "{}"',
                    OCF_RESOURCE_INSTANCE, is_in_recovery)
        return OCF_ERR_CONFIGURED
    elif rc in (1, 2):
        # psql cound not connect to the instance.
        # As pg_isready reported the instance was listening, this error
        # could be a max_connection saturation. Just report a soft error.
        ocf_log_err('_confirm_role: psql could not connect to instance "{}"',
                    OCF_RESOURCE_INSTANCE)
        return OCF_ERR_GENERIC

    # The query failed (rc: 3) or bad parameters (rc: -1).
    # This should not happen, raise a hard configuration error.
    ocf_log_err(
        '_confirm_role: the query to check if instance "{}" is a primary or '
        'a secondary failed (rc: {})', OCF_RESOURCE_INSTANCE, rc)
    return OCF_ERR_CONFIGURED


# Parse and return the current status of the local PostgreSQL instance as
# reported by its controldata file
# WARNING: the status is NOT updated in case of crash.
def _controldata_state():
    status = qx(PGCTRLDATA + " " + datadir + " 2>/dev/null")
    p = re.compile(r"^Database cluster state:\s+(.*?)\s*$", re.M)
    finds = p.findall(status)
    if not finds:
        ocf_log_crit(
            '_controldata_state: could not read state from controldata '
            'file for "{}"', datadir)
        sys.exit(OCF_ERR_CONFIGURED)
    return finds[0]


# Use pg_controldata to check the state of the PostgreSQL server. This
# function returns codes depending on this state, so we can find whether the
# instance is a primary or a secondary, or use it to detect any inconsistency
# that could indicate the instance has crashed.
def _controldata():
    state = _controldata_state()
    while state != '':
        ocf_log_debug('_controldata: instance "{}" state is "{}"',
                      OCF_RESOURCE_INSTANCE, state)
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
        ocf_log_debug('_controldata: waiting for transitionnal '
                      'state "{}" to finish', state)
        sleep(1)
        state = _controldata_state()
    # If we reach this point, something went really wrong with this code or
    # pg_controldata.
    ocf_log_err(
        '_controldata: unable get instance "{}" state using pg_controldata.',
        OCF_RESOURCE_INSTANCE)
    return OCF_ERR_INSTALLED


# Check the postmaster.pid file and the postmaster process.
# WARNING: we do not distinguish the scenario where postmaster.pid does not
# exist from the scenario where the process is still alive. It should be ok
# though, as this is considered a hard error from monitor.
def _pg_ctl_status():
    rc = _runas([PGCTL, '--pgdata', pgdata, 'status'])
    # Possible error codes:
    #   3: postmaster.pid file does not exist OR it does but the process
    #      with the PID found in the file is not alive
    return rc


# Check to confirm if the instance is really stopped as _pg_isready stated
# and if it was propertly shut down.
#
def _confirm_stopped():
    # Check the postmaster process status.
    pgctlstatus_rc = _pg_ctl_status()
    if pgctlstatus_rc == 0:
        # The PID file exists and the process is available.
        # That should not be the case, return an error.
        ocf_log_err(
            '_confirm_stopped: instance "{}" is not listening, '
            'but the process referenced in postmaster.pid exists',
            OCF_RESOURCE_INSTANCE)
        return OCF_ERR_GENERIC
    # The PID file does not exist or the process is not available.
    ocf_log_debug(
        '_confirm_stopped: no postmaster process found for instance "{}"',
        OCF_RESOURCE_INSTANCE)
    if os.path.isfile(datadir + "/backup_label"):
        # We are probably on a freshly built secondary that was not started yet.
        ocf_log_debug('_confirm_stopped: backup_label file exists: probably '
                      'on a never started secondary')
        return OCF_NOT_RUNNING
    # Continue the check with pg_controldata.
    controldata_rc = _controldata()
    if controldata_rc == OCF_RUNNING_MASTER:
        # The controldata has not been updated to "shutdown".
        # It should mean we had a crash on a primary instance.
        ocf_log_err(
            '_confirm_stopped: instance "{}" controldata indicates a running '
            'primary instance, the instance has probably crashed',
            OCF_RESOURCE_INSTANCE)
        return OCF_FAILED_MASTER
    elif controldata_rc == OCF_SUCCESS:
        # The controldata has not been updated to "shutdown in recovery".
        # It should mean we had a crash on a secondary instance.
        # There is no "FAILED_SLAVE" return code, so we return a generic error.
        ocf_log_err(
            '_confirm_stopped: instance "{}" controldata indicates a running '
            'secondary instance, the instance has probably crashed',
            OCF_RESOURCE_INSTANCE)
        return OCF_ERR_GENERIC
    elif controldata_rc == OCF_NOT_RUNNING:
        # The controldata state is consistent, the instance was probably
        # propertly shut down.
        ocf_log_debug(
            '_confirm_stopped: instance "{}" controldata indicates '
            'that the instance was propertly shut down',
            OCF_RESOURCE_INSTANCE)
        return OCF_NOT_RUNNING
    # Something went wrong with the controldata check.
    ocf_log_err(
        '_confirm_stopped: could not get instance "{}" status from controldata '
        '(returned: {})', OCF_RESOURCE_INSTANCE, controldata_rc)
    return OCF_ERR_GENERIC


# Monitor the PostgreSQL instance
def pgsql_monitor():
    if ocf_is_probe():
        ocf_log_debug("pgsql_monitor: monitor is a probe")
    # First check, verify if the instance is listening.
    pgisready_rc = _pg_isready()
    if pgisready_rc == 0:
        # The instance is listening.
        # We confirm that the instance is up and return if it is a primary or a
        # secondary
        ocf_log_debug('pgsql_monitor: instance "{}" is listening',
                      OCF_RESOURCE_INSTANCE)
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
        ocf_log_debug(
            'pgsql_monitor: instance "{}" rejects connections - '
            'checking again...', OCF_RESOURCE_INSTANCE)
        controldata_rc = _controldata()
        if controldata_rc in (OCF_RUNNING_MASTER, OCF_SUCCESS):
            # This state indicates that pg_isready check should succeed.
            # We check again.
            ocf_log_debug(
                'pgsql_monitor: instance "{}" controldata shows a '
                'running status', OCF_RESOURCE_INSTANCE)
            pgisready_rc = _pg_isready()
            if pgisready_rc == 0:
                # Consistent with pg_controdata output.
                # We can check if the instance is primary or secondary
                ocf_log_debug('pgsql_monitor: instance "{}" is listening',
                              OCF_RESOURCE_INSTANCE)
                return _confirm_role()
            # Still not consistent, raise an error.
            # NOTE: if the instance is a warm standby, we end here.
            # TODO raise an hard error here ?
            ocf_log_err(
                'pgsql_monitor: instance "{}" controldata is not consistent '
                'with pg_isready (returned: {})',
                OCF_RESOURCE_INSTANCE, pgisready_rc)
            ocf_log_info(
                 'pgsql_monitor: if this instance is in warm standby, '
                 'this resource agent only supports hot standby')
            return OCF_ERR_GENERIC
        if controldata_rc == OCF_NOT_RUNNING:
            # This state indicates that pg_isready check should fail with rc 2.
            # We check again.
            pgisready_rc = _pg_isready()
            if pgisready_rc == 2:
                # Consistent with pg_controdata output.
                # We check the process status using pg_ctl status and check
                # if it was propertly shut down using pg_controldata.
                ocf_log_debug('pgsql_monitor: instance "{}" is not listening',
                              OCF_RESOURCE_INSTANCE)
                return _confirm_stopped()
            # Still not consistent, raise an error.
            # TODO raise an hard error here ?
            ocf_log_err(
                'pgsql_monitor: instance "{}" controldata is not consistent '
                'with pg_isready (returned: {})',
                OCF_RESOURCE_INSTANCE, pgisready_rc)
            return OCF_ERR_GENERIC
        # Something went wrong with the controldata check, hard fail.
        ocf_log_err(
            'pgsql_monitor: could not get instance "{}" status from '
            'controldata (returned: {})', OCF_RESOURCE_INSTANCE, controldata_rc)
        return OCF_ERR_INSTALLED
    elif pgisready_rc == 2:
        # The instance is not listening.
        # We check the process status using pg_ctl status and check
        # if it was propertly shut down using pg_controldata.
        ocf_log_debug('pgsql_monitor: instance "{}" is not listening',
                      OCF_RESOURCE_INSTANCE)
        return _confirm_stopped()
    elif pgisready_rc == 3:
        # No attempt was done, probably a syntax error.
        # Hard configuration error, we don't want to retry or failover here.
        ocf_log_err(
            'pgsql_monitor: unknown error while checking if instance "{}" '
            'is listening (returned {})', OCF_RESOURCE_INSTANCE, pgisready_rc)
        return OCF_ERR_CONFIGURED
    ocf_log_err(
        'pgsql_monitor: unexpected result when checking instance "{}" status',
        OCF_RESOURCE_INSTANCE)
    return OCF_ERR_GENERIC


# Create the recovery file based on the given template.
# Given template MUST at least contain:
#   standby_mode=on
#   primary_conninfo='...'
#   recovery_target_timeline = 'latest'
def _create_recovery_conf():
    u = pwd.getpwnam(system_user)
    uid, gid = u.pw_uid, u.pw_gid
    recovery_file = datadir + "/recovery.conf"
    ocf_log_debug('_create_recovery_conf: get replication configuration from '
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
        ocf_log_crit(
            '_create_recovery_conf: could not open file "{}"', recovery_tpl)
        sys.exit(OCF_ERR_CONFIGURED)
    ocf_log_debug('_create_recovery_conf: write the replication configuration '
                  'to "{}" file', recovery_file)
    try:
        # Write recovery.conf using configuration from the template file
        with open(recovery_file, "w") as fh:
            fh.write(recovery_conf)
    except:
        ocf_log_crit('_create_recovery_conf: Could not open file "{}"',
                     recovery_file)
        sys.exit(OCF_ERR_CONFIGURED)
    try:
        os.chown(recovery_file, uid, gid)
    except:
        ocf_log_crit('_create_recovery_conf: Could not set owner of "{}"',
                     recovery_file)
        sys.exit(OCF_ERR_CONFIGURED)


# get the timeout for the current action given from environment var
# Returns   timeout as integer
#           undef if unknown
def _get_action_timeout():
    timeout = env_else('OCF_RESKEY_CRM_meta_timeout')
    if timeout is not None:
        timeout = int(timeout) / 1000
        ocf_log_debug('_get_action_timeout: known timeout: {}', timeout)
        return timeout
    ocf_log_debug('_get_action_timeout: known timeout env var not set: {}')


# Start the local instance using pg_ctl
def _pg_ctl_start():
    # Add 60s to the timeout or use a 24h timeout fallback to make sure
    # Pacemaker will give up before us and take decisions
    timeout = _get_action_timeout()
    if timeout is None:
        timeout = 60 * 60 * 24 + 60
    else:
        timeout += 60
    cmd = [PGCTL, '--pgdata', pgdata, '-w', '--timeout', timeout, 'start']
    if start_opts:
        cmd.extend(['-o', start_opts])
    return _runas(cmd)


# Get, parse and return the resource master score on given node.
# Returns an empty string if not found.
# Returns undef on crm_master call on error
def _get_master_score(node=None):
    node_arg = '--node "{}"'.format(node) if node else ""
    rc, score = qx2(
        CRM_MASTER + " --quiet --get-value " + node_arg + " 2> /dev/null")
    if rc != 0:
        return ''
    return score.strip("\n")


# This subroutine checks if a master score is set for one of the relative clones
# in the cluster and the score is greater or equal of 0.
# Returns True if at least one master score >= 0 is found.
# Returns False otherwise
def _master_score_exists():
    partition_nodes = qx(CRM_NODE + " --partition").split()
    for node in partition_nodes:
        score = _get_master_score(node)
        if score != "" and int(score) > -1:
            return True
    return False


# Set the given attribute name to the given value.
# As setting an attribute is asynchronous, this will return as soon as the
# attribute is really set by attrd and available everywhere.
def _set_master_score(score, node=None):
    node_arg = '--node "{}"'.format(node) if node else ""
    qx(CRM_MASTER + " " + node_arg + " --quiet --update " + score)
    while True:
        tmp = _get_master_score(node)
        if tmp == score:
            break
        ocf_log_debug('_set_master_score: waiting to set score to "{}" '
                      '(currently "{}")...', score, tmp)
        sleep(0.1)


# Start the PostgreSQL instance as a *secondary*
def pgsql_start():
    rc = pgsql_monitor()
    prev_state = _controldata_state()
    # Instance must be running as secondary or being stopped.
    # Anything else is an error.
    if rc == OCF_SUCCESS:
        ocf_log_info('pgsql_start: instance "{}" already started',
                     OCF_RESOURCE_INSTANCE)
        return OCF_SUCCESS
    elif rc != OCF_NOT_RUNNING:
        ocf_log_err(
            'pgsql_start: unexpected state for instance "{}" (returned {})',
            OCF_RESOURCE_INSTANCE, rc)
        return OCF_ERR_GENERIC
    #
    # From here, the instance is NOT running for sure.
    #
    ocf_log_debug(
        'pgsql_start: instance "{}" is not running, starting it as a secondary',
        OCF_RESOURCE_INSTANCE)
    # Create recovery.conf from the template file.
    _create_recovery_conf()
    # Start the instance as a secondary.
    rc = _pg_ctl_start()
    if rc == 0:
        # Wait for the start to finish.
        while True:
            rc = pgsql_monitor()
            if rc != OCF_NOT_RUNNING:
                break
            sleep(1)
        if rc == OCF_SUCCESS:
            ocf_log_info('pgsql_start: instance "{}" started',
                         OCF_RESOURCE_INSTANCE)
            # Check if a master score exists in the cluster.
            # During the very first start of the cluster, no master score will
            # exists on any of the existing slaves, unless an admin designated
            # one of them using crm_master. If no master exists the cluster will
            # not promote a master among the slaves.
            # To solve this situation, we check if there is at least one master
            # score existing on one node in the cluster. Do nothing if at least
            # one master score is found among the clones of the resource. If no
            # master score exists, set a score of 1 only if the resource was a
            # shut downed master before the start.
            if prev_state == "shut down" and not _master_score_exists():
                ocf_log_info(
                    'pgsql_start: no master score around; set mine to 1')
                _set_master_score('1')
            return OCF_SUCCESS
        ocf_log_err('pgsql_start: instance "{}" is not running as a '
                    'slave (returned {})', OCF_RESOURCE_INSTANCE, rc)
        return OCF_ERR_GENERIC
    ocf_log_err('pgsql_start: instance "{}" failed to start (rc: {})',
                OCF_RESOURCE_INSTANCE, rc)
    return OCF_ERR_GENERIC

 
# Stop the PostgreSQL instance
def pgsql_stop():
    # Add 60s to the timeout or use a 24h timeout fallback to make sure
    # Pacemaker will give up before us and take decisions
    timeout = _get_action_timeout()
    if timeout is None:
        timeout = 60 * 60 * 24 + 60
    else:
        timeout += 60
    # Instance must be running as secondary or primary or being stopped.
    # Anything else is an error.
    rc = pgsql_monitor()
    if rc == OCF_NOT_RUNNING:
        ocf_log_info('pgsql_stop: instance "{}" already stopped',
                     OCF_RESOURCE_INSTANCE)
        return OCF_SUCCESS
    elif rc not in (OCF_SUCCESS, OCF_RUNNING_MASTER):
        ocf_log_warn(
            'pgsql_stop: unexpected state for instance "{}" (returned {})',
            OCF_RESOURCE_INSTANCE, rc)
        return OCF_ERR_GENERIC
    #
    # From here, the instance is running for sure.
    #
    ocf_log_debug('pgsql_stop: instance "{}" is running, stopping it',
                  OCF_RESOURCE_INSTANCE)
    # Try to quit with proper shutdown.
    rc = _runas([PGCTL, '--pgdata', pgdata, '-w', '--timeout', timeout,
                 '-m', 'fast', 'stop'])
    if rc == 0:
        # Wait for the stop to finish.
        while True:
            rc = pgsql_monitor()
            if rc != OCF_NOT_RUNNING:
                break
            sleep(1)
        ocf_log_info('pgsql_stop: instance "{}" stopped', OCF_RESOURCE_INSTANCE)
        return OCF_SUCCESS
    ocf_log_err('pgsql_stop: instance "{}" failed to stop', OCF_RESOURCE_INSTANCE)
    return OCF_ERR_GENERIC


def _get_priv_attr(name, node=None):
    node = "--node " + node if node else ''
    ans = qx(ATTRD_PRIV + " --name " + name + " " + node + " --query")
    p = re.compile(r'^name=".*" host=".*" value="(.*)"$')
    m = p.findall(ans)
    if m:
        return m[0]
    return ''


# Promote the secondary instance to primary
def pgsql_promote():
    rc = pgsql_monitor()
    if rc == OCF_SUCCESS:
        # Running as slave. Normal, expected behavior.
        ocf_log_debug('pgsql_promote: "{}" currently running as a standby',
                      OCF_RESOURCE_INSTANCE)
    elif rc == OCF_RUNNING_MASTER:
        # Already a master. Unexpected, but not a problem.
        ocf_log_info('pgsql_promote: "{}" already running as a primary',
                     OCF_RESOURCE_INSTANCE)
        return OCF_SUCCESS
    elif rc == OCF_NOT_RUNNING:  # INFO this is not supposed to happen.
        # Currently not running. Need to start before promoting.
        ocf_log_info('pgsql_promote: "{}" currently not running, starting it',
                     OCF_RESOURCE_INSTANCE)
        rc = pgsql_start()
        if rc != OCF_SUCCESS:
            ocf_log_err(
                'pgsql_promote: failed to start the instance "{}"',
                OCF_RESOURCE_INSTANCE)
            return OCF_ERR_GENERIC
    else:
        ocf_log_info(
            'pgsql_promote: unexpected error, cannot promote "{}"',
            OCF_RESOURCE_INSTANCE)
        return OCF_ERR_GENERIC
    #
    # At this point, the instance **MUST** be started as a secondary.
    #
    # Cancel the switchover if it has been considered not safe during the
    # pre-promote action
    if _get_priv_attr('cancel_switchover') == '1':
        ocf_log_err('pgsql_promote: switchover has been canceled '
                    'from pre-promote action')
        _delete_priv_attr('cancel_switchover')
        return OCF_ERR_GENERIC
    # Do not check for a better candidate if we try to recover the master
    # Recover of a master is detected during the pre-promote action. It sets the
    # private attribute 'recover_master' to '1' if this is a master recover.
    if _get_priv_attr('recover_master') == '1':
        ocf_log_info('pgsql_promote: recovering old master, no election needed')
    else:
        # The promotion is occurring on the best known candidate (highest
        # master score), as chosen by pacemaker during the last working monitor
        # on previous master (see pgsql_monitor/_check_locations subs).
        # To avoid any race condition between the last monitor action on the
        # previous master and the **real** most up-to-date standby, we
        # set each standby location during the "pre-promote" action, and stored
        # them using the "lsn_location" resource attribute.
        #
        # The best standby to promote would have the highest known LSN. If the
        # current resource is not the best one, we need to modify the master
        # scores accordingly, and abort the current promotion.
        ocf_log_debug('pgsql_promote: checking if current node '
                      'is the best candidate for promotion')
        # Exclude nodes that are known to be unavailable (not in the current
        # partition) using the "crm_node" command
        active_nodes = _get_priv_attr('nodes').split()
        node_to_promote = ''
        # Get the "lsn_location" attribute value for the current node, as set
        # during the "pre-promote" action.
        # It should be the greatest among the secondary instances.
        max_lsn = _get_priv_attr('lsn_location')
        if max_lsn == '':
            # This should not happen as the "lsn_location" attribute should have
            # been updated during the "pre-promote" action.
            ocf_log_crit('pgsql_promote: can not get current node LSN location')
            return OCF_ERR_GENERIC
        # convert location to decimal
        max_lsn = max_lsn.strip("\n")
        wal_num, wal_off = max_lsn.split('/')
        max_lsn_dec = (294967296 * hex(wal_num)) + hex(wal_off)
        ocf_log_debug('pgsql_promote: current node lsn location: {}({})',
                      max_lsn, max_lsn_dec)
        # Now we compare with the other available nodes.
        for node in active_nodes:
            # We exclude the current node from the check.
            if node == nodename:
                continue
            # Get the "lsn_location" attribute value for the node, as set during
            # the "pre-promote" action.
            node_lsn = _get_priv_attr('lsn_location', node)
            if node_lsn == '':
                # This should not happen as the "lsn_location" attribute should
                # have been updated during the "pre-promote" action.
                ocf_log_crit(
                    'pgsql_promote: can not get LSN location for "{}"', node)
                return OCF_ERR_GENERIC
            # convert location to decimal
            node_lsn = node_lsn.strip('\n')
            wal_num, wal_off = node_lsn.split('/')
            node_lsn_dec = (4294967296 * hex(wal_num)) + hex(wal_off)
            ocf_log_debug('pgsql_promote: comparing with "{}": lsn is {}({})',
                          node, node_lsn, node_lsn_dec)
            # If the node has a bigger delta, select it as a best candidate to
            # promotion.
            if node_lsn_dec > max_lsn_dec:
                node_to_promote = node
                max_lsn_dec = node_lsn_dec
                # max_lsn = node_lsn
                ocf_log_debug(
                    'pgsql_promote: found "{}" is a better candidate to promote',
                    node)
        # If any node has been selected, we adapt the master scores accordingly
        # and break the current promotion.
        if node_to_promote != '':
            ocf_log_info(
                'pgsql_promote: {} is the best candidate to promote, '
                'aborting current promotion',
                node_to_promote)
            # Reset current node master score.
            _set_master_score('1')
            # Set promotion candidate master score.
            _set_master_score('1000', node_to_promote)
            # We fail the promotion to trigger another promotion transition
            # with the new scores.
            return OCF_ERR_GENERIC
        # Else, we will keep on promoting the current node.
    if not _runas([PGCTL, '--pgdata', pgdata, '-w', 'promote']) == 0:
        # Promote the instance on the current node.
        ocf_log_err('pgsql_promote: error during promotion')
        return OCF_ERR_GENERIC
    # The instance promotion is asynchronous, so we need to wait for this
    # process to complete.
    while pgsql_monitor() != OCF_RUNNING_MASTER:
        ocf_log_debug('pgsql_promote: waiting for the promote to complete')
        sleep(1)
    ocf_log_info('pgsql_promote: promote complete')
    return OCF_SUCCESS


# Demote the PostgreSQL instance from primary to secondary
# To demote a PostgreSQL instance, we must:
#   * stop it gracefully
#   * create recovery.conf with standby_mode = on
#   * start it
def pgsql_demote():
    rc = pgsql_monitor()
    # Running as primary. Normal, expected behavior.
    if rc == OCF_RUNNING_MASTER:
        ocf_log_debug('pgsql_demote: "{}" currently running as a primary',
                      OCF_RESOURCE_INSTANCE)
    elif rc == OCF_SUCCESS:
        # Already running as secondary. Nothing to do.
        ocf_log_debug('pgsql_demote: "{}" currently running as a secondary',
                      OCF_RESOURCE_INSTANCE)
        return OCF_SUCCESS
    elif rc == OCF_NOT_RUNNING:
        # Instance is stopped. Nothing to do.
        ocf_log_debug('pgsql_demote: "{}" currently shut down',
                      OCF_RESOURCE_INSTANCE)
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
        # Add 60s to the timeout or use a 24h timeout fallback to make sure
        # Pacemaker will give up before us and take decisions
        timeout = _get_action_timeout()
        if timeout is None:
            timeout = 60 * 60 * 24 + 60
        else:
            timeout += 60
        # WARNING the instance **MUST** be stopped gracefully.
        # Do **not** use pg_stop() or service or systemctl here as these
        # commands might force-stop the PostgreSQL instance using immediate
        # after some timeout and return success, which is misleading.
        rc = _runas([PGCTL, '--pgdata', pgdata, '--mode', 'fast', '-w',
                     '--timeout', timeout, 'stop'])
        # No need to wait for stop to complete, this is handled in pg_ctl
        # using -w option.
        if rc != 0:
            ocf_log_err('pgsql_demote: failed to stop "{}" using pg_ctl (returned {})',
                        OCF_RESOURCE_INSTANCE, rc)
            return OCF_ERR_GENERIC
        # Double check that the instance is stopped correctly.
        rc = pgsql_monitor()
        if rc != OCF_NOT_RUNNING:
            ocf_log_err('pgsql_demote: unexpected "{}" state: monitor status '
                        '({}) disagree with pg_ctl return code',
                        OCF_RESOURCE_INSTANCE, rc)
            return OCF_ERR_GENERIC
    #
    # At this point, the instance **MUST** be stopped gracefully.
    #
    # Note: We do not need to handle the recovery.conf file here as pgsql_start
    # deal with that itself. Equally, no need to wait for the start to complete
    # here, handled in pgsql_start.
    rc = pgsql_start()
    if rc == OCF_SUCCESS:
        ocf_log_info('pgsql_demote: "{}" started as a secondary',
                     OCF_RESOURCE_INSTANCE)
        return OCF_SUCCESS
    # NOTE: No need to double check the instance state as pgsql_start already use
    # pgsql_monitor to check the state before returning.
    ocf_log_err(
        'pgsql_demote: starting "{}" as a standby failed (returned {})',
        OCF_RESOURCE_INSTANCE, rc)
    return OCF_ERR_GENERIC


# Notify type actions, called on all available nodes before (pre) and after
# (post) other actions, like promote, start, ...
def pgsql_notify():
    ocf_log_debug("pgsql_notify: environment variables: {}",
                  pprint.pformat(OCF_NOTIFY_ENV))
    if not OCF_NOTIFY_ENV:
        return
    type_op = OCF_NOTIFY_ENV['type'] + "-" + OCF_NOTIFY_ENV['operation']
    if type_op == "pre-promote":
        return pgsql_notify_pre_promote()
    if type_op == "post-promote":
        return pgsql_notify_post_promote()
    if type_op == "pre-demote":
        return pgsql_notify_pre_demote()
    if type_op == "pre-stop$":
        return pgsql_notify_pre_stop()
    return OCF_SUCCESS


# Check if the current transiation is a recover of a master clone on given node.
def _is_master_recover(n):
    # n == OCF_NOTIFY_ENV['promote'][0]['uname']
    t1 = any(m['uname'] == n for m in OCF_NOTIFY_ENV['master'])
    t2 = any(m['uname'] == n for m in OCF_NOTIFY_ENV['promote'])
    return t1 and t2


# Check if the current transition is a recover of a slave clone on given node.
def _is_slave_recover(n):
    t1 = any(m['uname'] == n for m in OCF_NOTIFY_ENV['slave'])
    t2 = any(m['uname'] == n for m in OCF_NOTIFY_ENV['start'])
    return t1 and t2


# check if th current transition is a switchover to the given node.
def _is_switchover(n):
    old = OCF_NOTIFY_ENV['master'][0]['uname']
    if (len(OCF_NOTIFY_ENV['master']) != 1 or
        len(OCF_NOTIFY_ENV['demote']) != 1 or
            len(OCF_NOTIFY_ENV['promote']) != 1):
        return 0
    t1 = any(m['uname'] == old for m in OCF_NOTIFY_ENV['demote'])
    t2 = any(m['uname'] == n for m in OCF_NOTIFY_ENV['slave'])
    t3 = any(m['uname'] == n for m in OCF_NOTIFY_ENV['promote'])
    t4 = any(m['uname'] == old for m in OCF_NOTIFY_ENV['stop'])
    return t1 and t2 and t3 and not t4


# Set the given private attribute name to the given value
# As setting an attribute is asynchronous, this will return as soon as the
# attribute is really set by attrd and available.
def _set_priv_attr(name, val):
    ret = qx_check_only(ATTRD_PRIV + " --name " + name + " --update " + val)
    while _get_priv_attr(name) != val:
        ocf_log_debug('_set_priv_attr: waiting to set "{}"...', name)
        sleep(0.1)
    return ret


# Delete the given private attribute.
# As setting an attribute is asynchronous, this will return as soon as the
# attribute is really deleted by attrd.
def _delete_priv_attr(name):
    qx(ATTRD_PRIV + " --name " + name + " --delete")
    while _get_priv_attr(name) != '':
        ocf_log_debug('_delete_priv_attr: waiting to delete "{}"...', name)
        sleep(0.1)


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
    ocf_log_info('_check_switchover: switchover in progress from "{}" to "{}".'
                 ' Need to check the last record in WAL',
                 OCF_NOTIFY_ENV['demote'][0]['uname'], nodename)
    # Force a checpoint to make sure the controldata shows the very last TL
    _query("CHECKPOINT")
    # check if we received the shutdown checkpoint of the master during its
    # demote process.
    # We need the last local checkpoint LSN and the last received LSN from
    # master to check in the WAL between these adresses if we have a
    # "checkpoint shutdown" using pg_xlogdump.
    ans = qx(PGCTRLDATA + " " + datadir + " 2>/dev/null")
    # Get the latest known TL
    p = re.compile(r"^Latest checkpoint's TimeLineID:\s+(\d+)\s*$", re.M)
    m = p.findall(ans)
    tl = m[0] if m else None
    # Get the latest local checkpoint
    p = re.compile(r"^Latest checkpoint's REDO location:\s+([0-9A-F/]+)\s*$", re.M)
    m = p.findall(ans)
    last_chk = m[0] if m else None
    # Get the last received LSN from master
    #  0/4000000
    rc, rs = _query("SELECT pg_last_xlog_receive_location()")
    if rc != 0:
        ocf_log_err('_check_switchover: could not query '
                    'last_xlog_receive_location ({})', rc)
        return 2
    last_lsn = rs[0][0] if rs and rs[0] else None
    if not (tl and last_chk and last_lsn):
        ocf_log_crit('_check_switchover: could not read last '
                     'checkpoint and timeline from controldata file!')
        ocf_log_debug('_check_switchover: XLOGDUMP parameters: '
                      'datadir:"{}", last_chk: "{}", tl: "{}", mast_lsn: "{}"',
                      datadir, last_chk, tl, last_lsn)
        return 2
    # force a checkpoint on the slave to flush the master's
    # shutdown checkpoint in the WAL
    rc, ans = qx2(PGXLOGDUMP + " --path " + datadir + " --timeline " + tl +
                  " --start " + last_chk + " --end " + last_lsn + " 2>&1")
    ocf_log_debug(
        '_check_switchover: XLOGDUMP rc: "{}", tl: "{}", last_chk: {}, '
        'last_lsn: {}, output: "{}"', rc, tl, last_chk, last_lsn, ans)
    p = re.compile(
        r"^rmgr: XLOG.*desc: [cC][hH][eE][cC][kK][pP][oO][iI][nN][tT]"
        r"(:|_SHUTDOWN) redo [0-9A-F/]+; tli " + tl + r";.*; shutdown$",
        re.M | re.DOTALL)
    if rc == 0 and p.search(ans):
        ocf_log_info(
            '_check_switchover: slave received the shutdown checkpoint',
            _controldata_state())
        return 0
    _set_priv_attr('cancel_switchover', '1')
    ocf_log_info('pgsql_notify: did not received the shutdown checkpoint from '
                 'the old master!')
    return 1


# This action is called **before** the actual promotion when a failing master is
# considered unreclaimable, recoverable or a new master must be promoted
# (switchover or first start).
# As every "notify" action, it is executed almost simultaneously on all
# available nodes.
def pgsql_notify_pre_promote():
    ocf_log_info('pgsql_notify: promoting instance on node "{}"',
                 OCF_NOTIFY_ENV['promote'][0]['uname'])
    # No need to do an election between slaves if this is recovery of the master
    if _is_master_recover(OCF_NOTIFY_ENV['promote'][0]['uname']):
        ocf_log_warn('pgsql_notify: This is a master recovery!')
        if OCF_NOTIFY_ENV['promote'][0]['uname'] == nodename:
            _set_priv_attr('recover_master', '1')
        return OCF_SUCCESS
    # Environment cleanup!
    _delete_priv_attr('lsn_location')
    _delete_priv_attr('recover_master')
    _delete_priv_attr('nodes')
    _delete_priv_attr('cancel_switchover')
    # check for the last received entry of WAL from the master if we are
    # the designated slave to promote
    if _is_switchover(nodename) and any(m['uname'] == nodename
                                        for m in OCF_NOTIFY_ENV['promote']):
        rc = _check_switchover()
        if rc == 1:
            # Shortcut the election process as the switchover will be
            # canceled
            return OCF_SUCCESS
        elif rc != 0:
            # This is an extreme mesure, it shouldn't happen.
            qx(CRM_FAILCOUNT + " --resource " + OCF_RESOURCE_INSTANCE +
               " -v 1000000")
            return OCF_ERR_INSTALLED
        # If the sub keeps going, that means the switchover is safe.
        # Keep going with the election process in case the switchover was
        # instruct to the wrong node.
        # FIXME: should we allow a switchover to a lagging slave?
    # We need to trigger an election between existing slaves to promote the best
    # one based on its current LSN location. The designated standby for
    # promotion is responsible to connect to each available nodes to check their
    # "lsn_location".
    #
    # During the following promote action, pgsql_promote will use this
    # information to check if the instance to be promoted is the best one,
    # so we can avoid a race condition between the last successful monitor
    # on the previous master and the current promotion.
    rc, rs = _query('SELECT pg_last_xlog_receive_location()')

    if rc != 0:
        ocf_log_warn('pgsql_notify: could not query the current node LSN')
        # Return code are ignored during notifications...
        return OCF_SUCCESS
    node_lsn = rs[0][0]
    ocf_log_info('pgsql_notify: current node LSN: {}', node_lsn)
    # Set the "lsn_location" attribute value for this node so we can use it
    # during the following "promote" action.
    if not _set_priv_attr('lsn_location', node_lsn):
        ocf_log_warn(
            'pgsql_notify: could not set the current node LSN')
    # If this node is the future master, keep track of the slaves that
    # received the same notification to compare our LSN with them during
    # promotion
    active_nodes = defaultdict(int)
    if OCF_NOTIFY_ENV['promote'][0]['uname'] == nodename:
        # build the list of active nodes:
        #   master + slave + start - stop
        for foo in OCF_NOTIFY_ENV['master']:
            active_nodes[foo['uname']] += 1
        for foo in OCF_NOTIFY_ENV['slave']:
            active_nodes[foo['uname']] += 1
        for foo in OCF_NOTIFY_ENV['start']:
            active_nodes[foo['uname']] += 1
        for foo in OCF_NOTIFY_ENV['stop']:
            active_nodes[foo['uname']] -= 1
        attr_nodes = " ".join(k for k in active_nodes if active_nodes[k] > 0)
        _set_priv_attr('nodes', attr_nodes)
    return OCF_SUCCESS


# This action is called after a promote action.
def pgsql_notify_post_promote():
    # We have a new master (or the previous one recovered).
    # Environment cleanup!
    _delete_priv_attr('lsn_location')
    _delete_priv_attr('recover_master')
    _delete_priv_attr('nodes')
    _delete_priv_attr('cancel_switchover')
    return OCF_SUCCESS


# This is called before a demote occurs.
def pgsql_notify_pre_demote():
    # do nothing if the local node will not be demoted
    if not any(m['uname'] == nodename for m in OCF_NOTIFY_ENV["demote"]):
        return OCF_SUCCESS
    rc = pgsql_monitor()
    # do nothing if this is not a master recovery
    if not (_is_master_recover(nodename) and rc == OCF_FAILED_MASTER):
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
    ocf_log_info('pgsql_notify: trying to start failing master "{}"...',
                 OCF_RESOURCE_INSTANCE)
    # Either the instance managed to start or it couldn't.
    # We rely on the pg_ctk '-w' switch to take care of this. If it couldn't
    # start, this error will be catched up later during the various checks
    _pg_ctl_start()
    ocf_log_info('pgsql_notify: state is "{}" after recovery attempt',
                 _controldata_state())
    return OCF_SUCCESS


# This is called before a stop occurs.
def pgsql_notify_pre_stop():
    # do nothing if the local node will not be stopped
    if not any(m['uname'] == nodename for m in OCF_NOTIFY_ENV["stop"]):
        return OCF_SUCCESS
    rc = _controldata()
    # do nothing if this is not a slave recovery
    if not (_is_slave_recover(nodename) and rc == OCF_RUNNING_SLAVE):
        return OCF_SUCCESS
    # in case of slave crash, we need to detect if the CRM tries to recover
    # the slaveclone. The usual transition is to do: stop->start
    #
    # This transition can no twork because the instance is in
    # OCF_ERR_GENERIC step. So the stop action will fail, leading most
    # probably to fencing action.
    #
    # To avoid this, we try to start the instance in recovery from here.
    # If it success, at least it will be stopped correctly with a normal
    # status. If it fails, it will be catched up in next steps.
    ocf_log_info('pgsql_notify: trying to start failing slave "{}"...',
                 OCF_RESOURCE_INSTANCE)
    # Either the instance managed to start or it couldn't.
    # We rely on the pg_ctk '-w' switch to take care of this. If it couldn't
    # start, this error will be catched up later during the various checks
    _pg_ctl_start()
    ocf_log_info('pgsql_notify: state is "{}" after recovery attempt',
                 _controldata_state())
    return OCF_SUCCESS


# Action used to allow for online modification of resource parameters value.
def pgsql_reload():
    # No action necessary, the action declaration is enough to inform pacemaker
    # that the modification of any non-unique parameter can be applied without
    # having to restart the resource.
    ocf_log_info('pgsql_reload: instance "{}" reloaded', OCF_RESOURCE_INSTANCE)
    return OCF_SUCCESS


def ocf_meta_data():
    print("""\<?xml version="1.0"?>
        <!DOCTYPE resource-agent SYSTEM "ra-api-1.dtd">
        <resource-agent name="pgsqlsr">
          <version>1.0</version>

          <longdesc lang="en">
            Resource script for PostgreSQL in replication. It manages PostgreSQL servers using streaming replication as an HA resource.
          </longdesc>
          <shortdesc lang="en">Manages PostgreSQL servers in replication</shortdesc>
          <parameters>
            <parameter name="system_user" unique="0" required="0">
              <longdesc lang="en">
                System user account used to run the PostgreSQL server
              </longdesc>
              <shortdesc lang="en">PostgreSQL system User</shortdesc>
              <content type="string" default="$system_user_default" />
            </parameter>

            <parameter name="bindir" unique="0" required="0">
              <longdesc lang="en">
                Path to the directory storing the PostgreSQL binaries. The agent uses psql, pg_isready, pg_controldata and pg_ctl.
              </longdesc>
              <shortdesc lang="en">Path to the PostgreSQL binaries</shortdesc>
              <content type="string" default="$bindir_default" />
            </parameter>

            <parameter name="pgdata" unique="1" required="0">
              <longdesc lang="en">
                Path to the data directory, e.g. PGDATA
              </longdesc>
              <shortdesc lang="en">Path to the data directory</shortdesc>
              <content type="string" default="$pgdata_default" />
            </parameter>

            <parameter name="datadir" unique="1" required="0">
              <longdesc lang="en">
                Path to the directory set in data_directory from your postgresql.conf file. This parameter
                has the same default than PostgreSQL itself: the pgdata parameter value. Unless you have a
                special PostgreSQL setup and you understand this parameter, ignore it.
              </longdesc>
              <shortdesc lang="en">Path to the directory set in data_directory from your postgresql.conf file</shortdesc>
              <content type="string" default="PGDATA" />
            </parameter>

            <parameter name="pghost" unique="0" required="0">
              <longdesc lang="en">
                Host IP address or unix socket folder the instance is listening on.
              </longdesc>
              <shortdesc lang="en">Instance IP or unix socket folder</shortdesc>
              <content type="string" default="$pghost_default" />
            </parameter>

            <parameter name="pgport" unique="0" required="0">
              <longdesc lang="en">
                Port the instance is listening on.
              </longdesc>
              <shortdesc lang="en">Instance port</shortdesc>
              <content type="integer" default="$pgport_default" />
            </parameter>

            <parameter name="recovery_template" unique="1" required="0">
              <longdesc lang="en">
                Path to the recovery.conf template. This file is simply copied to \$PGDATA
                before starting the instance as slave
              </longdesc>
              <shortdesc lang="en">Path to the recovery.conf template.</shortdesc>
              <content type="string" default="PGDATA/recovery.conf.pcmk" />
            </parameter>

            <parameter name="start_opts" unique="0" required="0">
              <longdesc lang="en">
                Additionnal arguments given to the postgres process on startup.
                See "postgres --help" for available options. Usefull when the
                postgresql.conf file is not in the data directory (PGDATA), eg.:
                "-c config_file=/etc/postgresql/9.3/main/postgresql.conf".
              </longdesc>
              <shortdesc lang="en">Additionnal arguments given to the postgres process on startup.</shortdesc>
              <content type="string" default="$start_opts_default" />
            </parameter>

          </parameters>
          <actions>
            <action name="start" timeout="60" />
            <action name="stop" timeout="60" />
            <action name="status" timeout="20" />
            <action name="reload" timeout="20" />
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
        </resource-agent>
    """)


def ocf_methods():
    print("""\
        start
        stop
        reload
        promote
        demote
        monitor
        notify
        methods
        meta-data
        validate-all
    """)


def hadate():
    return datetime.datetime.now().strftime(HA_DATEFMT)


def set_logtag():
    if env_else('HA_LOGTAG', "") != "":
        return
    inst = env_else('OCF_RESOURCE_INSTANCE', "")
    if inst != "":
        os.environ['HA_LOGTAG'] = "{}({})[{}]".format(
            __SCRIPT_NAME, inst, os.getpid())
    else:
        os.environ['HA_LOGTAG'] = "{}[{}]".format(
            __SCRIPT_NAME, os.getpid())


if __name__ == "__main__":
    INITDIR = env_else('INITDIR', '/etc/init.d')
    HA_DIR = env_else('HA_DIR', '/etc/ha.d')
    HA_RCDIR = env_else('HA_RCDIR', '/etc/ha.d/rc.d')
    HA_CONFDIR = env_else('HA_CONFDIR', '/etc/ha.d/conf')
    HA_CF = env_else('HA_CF', '/etc/ha.d/ha.cf')
    HA_VARLIB = env_else('HA_VARLIB', '/var/lib/heartbeat')
    HA_RSCTMP = env_else('HA_RSCTMP', '/var/run/resource-agents')
    HA_RSCTMP_OLD = env_else('HA_RSCTMP_OLD', '/var/run/heartbeat/rsctmp')
    HA_FIFO = env_else('HA_FIFO', '/var/lib/heartbeat/fifo')
    HA_BIN = env_else('HA_BIN', '/usr/libexec/heartbeat')
    HA_SBIN_DIR = env_else('HA_SBIN_DIR', '/usr/sbin')
    HA_DATEFMT = env_else('HA_DATEFMT', '%Y/%m/%d_%T ')
    HA_DEBUGLOG = env_else('HA_DEBUGLOG', '/dev/null')
    HA_RESOURCEDIR = env_else('HA_RESOURCEDIR', '/etc/ha.d/resource.d')
    HA_DOCDIR = env_else('HA_DOCDIR', '/usr/share/doc/heartbeat')
    HA_VARRUN = env_else('HA_VARRUN', '/var/run/')
    HA_VARLOCK = env_else('HA_VARLOCK', '/var/lock/subsys/')
    __SCRIPT_NAME = os.environ.get('__SCRIPT_NAME', None)
    if not __SCRIPT_NAME:
        if __file__:
            __SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]

    ocf_prefix = '/usr'
    ocf_exec_prefix = '/usr'

    __OCF_ACTION = sys.argv[1]
    del_env('LC_ALL')
    os.environ['LC_ALL'] = 'C'
    locale.setlocale(locale.LC_ALL, 'C')
    del_env('LANG')
    del_env('LANGUAGE')
    if env_else('OCF_ROOT', '') == '':
        os.environ["OCF_ROOT"] = '/usr/lib/ocf'
    if env_else('OCF_FUNCTIONS_DIR', '') == os.environ["OCF_ROOT"] + "/resource.d/heartbeat":
        del_env('OCF_FUNCTIONS_DIR')
    if 'OCF_RESKEY_CRM_meta_interval' not in os.environ:
        os.environ['OCF_RESKEY_CRM_meta_interval'] = "0"
    if env_else('$OCF_RESKEY_OCF_CHECK_LEVEL', '') != '':
        os.environ["OCF_CHECK_LEVEL"] = os.environ[
            "$OCF_RESKEY_OCF_CHECK_LEVEL"]
    else:
        os.environ["OCF_CHECK_LEVEL"] = "0"
    if not os.path.isdir(os.environ["OCF_ROOT"]):
        ha_log(
            "ERROR: OCF_ROOT points to non-directory " + os.environ["OCF_ROOT"])
        sys.exit(OCF_ERR_GENERIC)
    if env_else('OCF_RESOURCE_TYPE', '') == '':
        os.environ["OCF_RESOURCE_TYPE"] = __SCRIPT_NAME
    if env_else('OCF_RA_VERSION_MAJOR', '') == '':
        os.environ["OCF_RA_VERSION_MAJOR"] = 'default'
    else:
        if __OCF_ACTION == "meta-data":
            os.environ["OCF_RESOURCE_INSTANCE"] = "undef"
        if env_else('OCF_RESOURCE_INSTANCE', '') == '':
            ha_log("ERROR: Need to tell us our resource instance name.")
            sys.exit(OCF_ERR_ARGS)

    OCF_RESOURCE_INSTANCE = env_else('OCF_RESOURCE_INSTANCE', None)
    OCF_ACTION = sys.argv[1]
    OCF_RUNNING_SLAVE = OCF_SUCCESS
    OCF_NOTIFY_ENV = ocf_notify_env() if OCF_ACTION == 'notify' else None

    system_user_default = "postgres"
    bindir_default = "/usr/bin"
    pgdata_default = "/var/lib/pgsql/data"
    pghost_default = "/tmp"
    pgport_default = 5432
    start_opts_default = ""

    system_user = env_else('OCF_RESKEY_system_user', system_user_default)
    bindir = env_else('OCF_RESKEY_bindir', bindir_default)
    pgdata = env_else('OCF_RESKEY_pgdata', pgdata_default)
    datadir = env_else('OCF_RESKEY_datadir', pgdata)
    pghost = env_else('OCF_RESKEY_pghost', pghost_default)
    pgport = env_else('OCF_RESKEY_pgport', pgport_default)
    start_opts = env_else('OCF_RESKEY_start_opts', start_opts_default)
    recovery_tpl = env_else('OCF_RESKEY_recovery_template',
                            os.path.join(pgdata, "recovery.conf.pcmk"))

    PGCTL = os.path.join(bindir, "pg_ctl")
    PGPSQL = os.path.join(bindir, "psql")
    PGCTRLDATA = os.path.join(bindir, "pg_controldata")
    PGISREADY = os.path.join(bindir, "pg_isready")
    PGXLOGDUMP = os.path.join(bindir, "pg_xlogdump")

    CRM_MASTER = os.path.join(HA_SBIN_DIR, "crm_master") + " --lifetime forever"
    CRM_ATTRIBUTE = os.path.join(
        HA_SBIN_DIR, "crm_attribute") + " --lifetime reboot --type status"
    CRM_NODE = os.path.join(HA_SBIN_DIR, "crm_node")
    CRM_RESOURCE = os.path.join(HA_SBIN_DIR, "crm_resource")
    CRM_FAILCOUNT = os.path.join(HA_SBIN_DIR, "crm_failcount")
    ATTRD_PRIV = os.path.join(
        HA_SBIN_DIR, "attrd_updater") + " --private --lifetime reboot"
    os.chdir(gettempdir())
    nodename = ocf_local_nodename()
    if OCF_ACTION in ("start", "stop", "reload", "monitor",
                      "promote", "demote", "notify"):
        pgsql_validate_all()
        # No need to validate for meta-data, methods or validate-all.
    if OCF_ACTION == 'start':
        sys.exit(pgsql_start())
    if OCF_ACTION == 'stop':
        sys.exit(pgsql_stop())
    if OCF_ACTION == 'monitor':
        sys.exit(pgsql_monitor())
    if OCF_ACTION == 'promote':
        sys.exit(pgsql_promote())
    if OCF_ACTION == 'demote':
        sys.exit(pgsql_demote())
    if OCF_ACTION == 'notify':
        sys.exit(pgsql_notify())
    if OCF_ACTION == 'reload':
        sys.exit(pgsql_reload())
    if OCF_ACTION == 'validate-all':
        sys.exit(pgsql_validate_all())
    if OCF_ACTION == "meta-data":
        ocf_meta_data()
    elif OCF_ACTION == "methods":
        ocf_methods()
    else:
        sys.exit(OCF_ERR_UNIMPLEMENTED)
