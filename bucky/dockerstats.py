
import time
import docker
import logging
import multiprocessing

import six
import requests.exceptions


if six.PY3:
    xrange = range
    long = int


log = logging.getLogger(__name__)


class DockerStatsServer(multiprocessing.Process):
    def __init__(self, queue, cfg):
        super(DockerStatsServer, self).__init__()
        self.queue = queue
        self.metadata = {}
        if cfg.metadata:
            self.metadata.update(cfg.metadata)
        if cfg.docker_stats_metadata:
            self.metadata.update(cfg.docker_stats_metadata)
        self.interval = cfg.docker_stats_interval
        if cfg.docker_stats_version:
            self.docker_client = docker.client.from_env(version=cfg.docker_stats_version)
        else:
            self.docker_client = docker.client.from_env()

    def close(self):
        pass

    def run(self):
        err = 0
        while True:
            start_timestamp = time.time()
            if not self.read_docker_stats():
                err = min(err + 1, 2)
            else:
                err = 0
            stop_timestamp = time.time()
            sleep_time = (err + 1) * self.interval - (stop_timestamp - start_timestamp)
            if sleep_time > 0.1:
                time.sleep(sleep_time)

    def add_stat(self, name, value, timestamp, metadata):
        if metadata:
            if self.metadata:
                metadata.update(self.metadata)
        else:
            metadata = self.metadata
        if metadata:
            self.queue.put((None, name, value, timestamp, metadata))
        else:
            self.queue.put((None, name, value, timestamp))

    def _merge(self, *dicts):
        ret = {}
        for d in dicts:
            ret.update(d)
        return ret

    def _add_df_stats(self, now, labels, total_size, rw_size):
        docker_df_stats = {
            'total_bytes': long(total_size),
            'used_bytes': long(rw_size)
        }
        self.add_stat("docker_df", docker_df_stats, now, labels)

    def _add_cpu_stats(self, now, labels, stats):
        for k, v in enumerate(stats[u'percpu_usage']):
            self.add_stat("docker_cpu", {'usage': long(v)}, now, self._merge(labels, {"instance": k}))

    def _add_interface_stats(self, now, labels, stats):
        for k in stats.keys():
            v = stats[k]
            keys = (
                u'rx_bytes', u'rx_packets', u'rx_errors', u'rx_dropped',
                u'tx_bytes', u'tx_packets', u'tx_errors', u'tx_dropped'
            )
            docker_interface_stats = {k: long(v[k]) for k in keys}
            self.add_stat("docker_interface", docker_interface_stats, now, self._merge(labels, dict(instance=k)))

    def _add_memory_stats(self, now, labels, stats):
        self.add_stat("docker_memory", {'used_bytes': long(stats[u'usage'])}, now, labels)

    def read_docker_stats(self):
        now = int(time.time())
        try:
            for i, container in enumerate(self.docker_client.api.containers(size=True)):
                labels = container[u'Labels']
                if 'container_id' not in labels:
                    labels['container_id'] = container[u'Id'][:12]
                stats = self.docker_client.api.stats(container[u'Id'], decode=True, stream=False)
                self._add_df_stats(now, labels, long(container[u'SizeRootFs']), long(container.get(u'SizeRw', 0)))
                self._add_cpu_stats(now, labels, stats[u'cpu_stats'][u'cpu_usage'])
                self._add_memory_stats(now, labels, stats[u'memory_stats'])
                self._add_interface_stats(now, labels, stats[u'networks'])
            return True
        except requests.exceptions.ConnectionError:
            return False
        except ValueError:
            return False
