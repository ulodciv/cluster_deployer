import json
import logging
from argparse import ArgumentParser
from datetime import timedelta
from time import time

from deployer import Cluster


def main(args):
    start = time()
    with open(args.file) as f:
        cluster_def = json.load(f)
    cluster = Cluster(cluster_def, args.no_threads)
    cluster.deploy()
    print(f"took {timedelta(seconds=time() - start)}")


def parse_args():
    parser = ArgumentParser(description='Deploy a cluster')
    parser.add_argument("file", help="Cluster definition file (JSON)")
    parser.add_argument('--no-threads', action='store_true')
    return parser.parse_args()

if __name__ == "__main__":
    logging.basicConfig(format='%(message)s')
    main(parse_args())
