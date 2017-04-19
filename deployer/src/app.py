import json
import logging
from argparse import ArgumentParser
from datetime import timedelta
from subprocess import run, PIPE
from time import time

import sys

from deployer import Cluster
from deployer_error import DeployerError


def find_vboxmanage():
    res = run("where vboxmanage.exe", stdout=PIPE, stderr=PIPE)
    if not res.returncode:
        return res.stdout.decode().strip()
    f = r"C:\Program Files\Oracle\VirtualBox\vboxmanage.exe"
    try:
        if not run([f, "-v"], stdout=PIPE, stderr=PIPE).returncode:
            return f
    except FileNotFoundError:
        pass
    try:
        run(["vboxmanage.exe", "-v"], stdout=PIPE, stderr=PIPE)
        return "vboxmanage.exe"
    except FileNotFoundError:
        sys.exit("can't find vboxmanage.exe: is VBox installed?")


def configure_logging():
    logging.basicConfig(format='%(relativeCreated)d %(name)s: %(message)s')
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger("paramiko").setLevel(logging.WARNING)


def main(args):
    configure_logging()
    start = time()
    with open(args.file) as f:
        cluster_def = json.load(f)
    if args.vboxmanage:
        vboxmanage = args.vboxmanage
    else:
        vboxmanage = find_vboxmanage()
    cluster = Cluster(vboxmanage, cluster_def, args.no_threads)
    try:
        cluster.deploy()
    except DeployerError as e:
        sys.exit(f"deployer error: {e}")
    logging.getLogger("main").debug(
        f"done, took {timedelta(seconds=time() - start)}")


def parse_args():
    parser = ArgumentParser(description='Deploy a cluster')
    parser.add_argument("file", help="Cluster definition file (JSON)")
    parser.add_argument('--no-threads', action='store_true')
    parser.add_argument('--vboxmanage')
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())
