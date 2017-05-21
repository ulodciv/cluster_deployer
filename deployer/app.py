import json
import logging
import sys
from argparse import ArgumentParser
from datetime import timedelta
from time import time

from deployer_error import DeployerError
from cluster import VboxPgHaCluster


def main(args):
    start = time()
    try:
        with open(args.file) as f:
            cluster = VboxPgHaCluster(
                cluster_def=json.load(f), use_threads=args.use_threads)
        cluster.deploy()
    except DeployerError as e:
        sys.exit(f"deployer error: {e}")
    logging.getLogger("main").debug(
        f"done, took {timedelta(seconds=time() - start)}")


def parse_args():
    parser = ArgumentParser(description='Deploy a cluster')
    parser.add_argument("file", help="Cluster definition file (JSON)")
    parser.add_argument('--use-threads', action='store_true', default=True)
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())
