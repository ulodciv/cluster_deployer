import logging
from datetime import timedelta
from os.path import join, expanduser
from time import time

from deployer import Cluster

HOME_DIR = expanduser("~")
IP_PREFIX = "192.168.72"
NAME_PREFIX = "centos-pg-"
BASE_HOST = {
    "vm_ova": join(HOME_DIR, r"dev\centos7_2.ova"),
    # "vm_ova": join(HOME_DIR, r"dev\debian9_1.ova"),
    "cluster": "PostgresCluster",
    "cluster_vip": f"{IP_PREFIX}.201",
    "root_password": "root",
    "users": ["root", "postgres", "user1", "repl1"],
    "pg_replication_user": "repl1",
    "pg_root_user": "postgres",
}
HOSTS = [
    {
        **BASE_HOST,
        "name": f"{NAME_PREFIX}1",
        "static_ip": f"{IP_PREFIX}.101",
    },
    {
        **BASE_HOST,
        "name": f"{NAME_PREFIX}2",
        "static_ip": f"{IP_PREFIX}.102",
    },
    {
        **BASE_HOST,
        "name": f"{NAME_PREFIX}3",
        "static_ip": f"{IP_PREFIX}.103",
    }
]
HOST1 = {
    **BASE_HOST,
    "name": "pg02",
    "static_ip": f"192.168.72.102",
}


def main():
    start = time()
    cluster = Cluster(HOSTS)
    cluster.deploy()
    print(f"took {timedelta(seconds=time() - start)}")
    # vm1 = MyVm(**HOSTS[1])
    # vm1.assume_ip_is_static()
    # vm1.vm_start_and_get_ip(False)
    # print(vm1.iface)
    # print(vm1.get_distro())
    # vm1.set_static_ip()
    # vm1.assume_ip_is_static()

if __name__ == "__main__":
    logging.basicConfig(format='%(message)s')
    main()
