
import time
import docker
import logging
import multiprocessing

import six


if six.PY3:
    xrange = range
    long = int


log = logging.getLogger(__name__)


class DockerStatsServer(multiprocessing.Process):
    def __init__(self, queue, cfg):
        super(DockerStatsServer, self).__init__()
        self.queue = queue
        self.metadata = cfg.docker_stats_metadata
        self.interval = cfg.docker_stats_interval
        self.docker_client = docker.client.from_env()

    def close(self):
        pass

    def run(self):
        while True:
            start_timestamp = time.time()
            self.read_docker_stats()
            stop_timestamp = time.time()
            sleep_time = self.interval - (stop_timestamp - start_timestamp)
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
        self.add_stat("docker_df", long(total_size), now,
                      self._merge(labels, dict(type="bytes", description="total")))
        self.add_stat("docker_df", long(rw_size), now,
                      self._merge(labels, dict(type="bytes", description="used")))

    def _add_cpu_stats(self, now, labels, stats):
        for k, v in enumerate(stats[u'percpu_usage']):
            self.add_stat("docker_cpu", long(v), now,
                          self._merge(labels, {"instance": k, "type": "usage"}))

    def _add_interface_stats(self, now, labels, stats):
        for k in stats.keys():
            v = stats[k]
            self.add_stat("docker_interface", long(v[u'rx_bytes']), now,
                          self._merge(labels, dict(instance=k, direction="rx", type="bytes")))
            self.add_stat("docker_interface", long(v[u'rx_packets']), now,
                          self._merge(labels, dict(instance=k, direction="rx", type="packets")))
            self.add_stat("docker_interface", long(v[u'rx_errors']), now,
                          self._merge(labels, dict(instance=k, direction="rx", type="errors")))
            self.add_stat("docker_interface", long(v[u'rx_dropped']), now,
                          self._merge(labels, dict(instance=k, direction="rx", type="drops")))
            self.add_stat("docker_interface", long(v[u'tx_bytes']), now,
                          self._merge(labels, dict(instance=k, direction="tx", type="bytes")))
            self.add_stat("docker_interface", long(v[u'tx_packets']), now,
                          self._merge(labels, dict(instance=k, direction="tx", type="packets")))
            self.add_stat("docker_interface", long(v[u'tx_errors']), now,
                          self._merge(labels, dict(instance=k, direction="tx", type="errors")))
            self.add_stat("docker_interface", long(v[u'tx_dropped']), now,
                          self._merge(labels, dict(instance=k, direction="tx", type="drops")))

    def _add_memory_stats(self, now, labels, stats):
        self.add_stat("docker_memory", long(stats[u'usage']), now,
                      self._merge(labels, dict(type="used", description="bytes")))

    def read_docker_stats(self):
        now = int(time.time())
        for container in self.docker_client.api.containers(size=True):
            labels = container[u'Labels']
            stats = self.docker_client.api.stats(container[u'Id'], decode=True, stream=False)
            self._add_df_stats(now, labels, long(container[u'SizeRootFs']), long(container.get(u'SizeRw', 0)))
            self._add_cpu_stats(now, labels, stats[u'cpu_stats'][u'cpu_usage'])
            self._add_memory_stats(now, labels, stats[u'memory_stats'])
            self._add_interface_stats(now, labels, stats[u'networks'])
