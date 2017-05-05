import json
import logging
import sys
from argparse import ArgumentParser
from datetime import timedelta
from time import time

from deployer import Cluster
from deployer_error import DeployerError


def main(args):
    start = time()
    try:
        with open(args.file) as f:
            cluster = Cluster(args.vboxmanage, json.load(f), args.no_threads)
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
