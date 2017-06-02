import logging
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

from paramiko import RSAKey


def raise_first(futures):
    for future in as_completed(futures):
        if future.exception():
            raise future.exception()


class ClusterBase:

    @staticmethod
    def configure_logging():
        logging.basicConfig(format='%(asctime)s %(name)s: %(message)s')
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("paramiko").setLevel(logging.WARNING)

    def __init__(self, *, cluster_def, vm_class, use_threads=True, **kwargs):
        super(ClusterBase, self).__init__(**kwargs)
        self.use_threads = use_threads
        self.configure_logging()
        common = {k: v for k, v in cluster_def.items() if k != "hosts"}
        common["paramiko_key"] = RSAKey.from_private_key_file(
            common["key_file"])
        with open(common["pub_key_file"]) as f:
            common["paramiko_pub_key"] = f.read()
        hosts = [
            {
                **common,
                **host
            } for host in cluster_def["hosts"]
        ]
        self.vms = [vm_class(**h) for h in hosts]

    def call(self, calls):
        if not self.use_threads:
            [c() for c in calls]
        else:
            with ThreadPoolExecutor() as e:
                raise_first([e.submit(c) for c in calls])

    @abstractmethod
    def deploy(self):
        pass

    def deploy_base(self):
        """ can be re-run """
        self.call([partial(v.deploy, self.vms) for v in self.vms])
        self.call([partial(v.authorize_keys, self.vms) for v in self.vms])
        self.call([partial(v.add_fingerprints, self.vms) for v in self.vms])
