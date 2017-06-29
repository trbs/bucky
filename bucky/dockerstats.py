
import time
import docker
import logging
import bucky.collector as collector

import six
import requests.exceptions


if six.PY3:
    xrange = range
    long = int


log = logging.getLogger(__name__)


class DockerStatsCollector(collector.StatsCollector):
    def __init__(self, queue, cfg):
        super(DockerStatsCollector, self).__init__(queue)
        self.metadata = self.merge_dicts(cfg.metadata, cfg.docker_stats_metadata)
        self.interval = cfg.docker_stats_interval
        if cfg.docker_stats_version:
            self.docker_client = docker.client.from_env(version=cfg.docker_stats_version)
        else:
            self.docker_client = docker.client.from_env()

    def read_df_stats(self, now, labels, total_size, rw_size):
        docker_df_stats = {
            'total_bytes': long(total_size),
            'used_bytes': long(rw_size)
        }
        self.add_stat("docker_filesystem", docker_df_stats, now, **labels)

    def read_cpu_stats(self, now, labels, stats):
        for k, v in enumerate(stats[u'percpu_usage']):
            self.add_stat("docker_cpu", {'usage': long(v)}, now, instance=k, **labels)

    def read_interface_stats(self, now, labels, stats):
        for k in stats.keys():
            v = stats[k]
            keys = (
                u'rx_bytes', u'rx_packets', u'rx_errors', u'rx_dropped',
                u'tx_bytes', u'tx_packets', u'tx_errors', u'tx_dropped'
            )
            docker_interface_stats = {k: long(v[k]) for k in keys}
            self.add_stat("docker_interface", docker_interface_stats, now, instance=k, **labels)

    def read_memory_stats(self, now, labels, stats):
        self.add_stat("docker_memory", {'used_bytes': long(stats[u'usage'])}, now, **labels)

    def collect(self):
        now = int(time.time())
        try:
            for i, container in enumerate(self.docker_client.api.containers(size=True)):
                labels = container[u'Labels']
                if 'docker_id' not in labels:
                    labels['docker_id'] = container[u'Id'][:12]
                stats = self.docker_client.api.stats(container[u'Id'], decode=True, stream=False)
                self.read_df_stats(now, labels, long(container[u'SizeRootFs']), long(container.get(u'SizeRw', 0)))
                self.read_cpu_stats(now, labels, stats[u'cpu_stats'][u'cpu_usage'])
                self.read_memory_stats(now, labels, stats[u'memory_stats'])
                self.read_interface_stats(now, labels, stats[u'networks'])
            return True
        except requests.exceptions.ConnectionError:
            return False
        except ValueError:
            return False
