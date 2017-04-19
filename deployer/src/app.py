import json
import logging
from argparse import ArgumentParser
from datetime import timedelta
from subprocess import check_output, CalledProcessError, STDOUT
from time import time

from deployer import Cluster
from deployer_error import DeployerError


def find_vboxmanage():
    try:
        return check_output("where vboxmanage.exe", stderr=STDOUT).decode().strip()
    except CalledProcessError:
        pass
    f = r"C:\Program Files\Oracle\VirtualBox\vboxmanage.exe"
    try:
        _ = check_output([f, "-v"], stderr=STDOUT)
        return f
    except CalledProcessError:
        pass
    try:
        _ = check_output(["vboxmanage.exe", "-v"], stderr=STDOUT)
        return "vboxmanage.exe"
    except FileNotFoundError:
        raise DeployerError("can't find vboxmanage.exe: is VBox installed?")


def main(args):
    logging.basicConfig(format='%(relativeCreated)d %(name)s: %(message)s')
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    start = time()
    with open(args.file) as f:
        cluster_def = json.load(f)
    if args.vboxmanage:
        vboxmanage = args.vboxmanage
    else:
        vboxmanage = find_vboxmanage()
    cluster = Cluster(vboxmanage, cluster_def, args.no_threads)
    cluster.deploy()
    logging.shutdown()
    print(f"took {timedelta(seconds=time() - start)}")


def parse_args():
    parser = ArgumentParser(description='Deploy a cluster')
    parser.add_argument("file", help="Cluster definition file (JSON)")
    parser.add_argument('--no-threads', action='store_true')
    parser.add_argument('--vboxmanage')
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())
