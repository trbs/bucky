
import os
import time
import logging
import multiprocessing

import six


if six.PY3:
    xrange = range
    long = int


log = logging.getLogger(__name__)


class SystemStatsServer(multiprocessing.Process):
    # The order of cpu fields in /proc/stat
    CPU_FIELDS = ('user', 'nice', 'system', 'idle', 'wait', 'interrupt', 'softirq', 'steal')

    def __init__(self, queue, cfg):
        super(SystemStatsServer, self).__init__()
        self.queue = queue
        self.metadata = cfg.system_stats_metadata
        self.interval = cfg.system_stats_interval

    def close(self):
        pass

    def run(self):
        while True:
            start_timestamp = time.time()
            self.read_cpu_stats()
            self.read_load_stats()
            self.read_df_stats()
            self.read_memory_stats()
            self.read_interface_stats()
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

    def read_cpu_stats(self):
        now = int(time.time())
        with open('/proc/stat') as f:
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens:
                    continue
                name = tokens[0]
                if not name.startswith('cpu'):
                    if name == 'ctxt':
                        self.add_stat("processes", long(tokens[1]), now, {"type": "switches"})
                    elif name == 'processes':
                        self.add_stat("processes", long(tokens[1]), now, {"type": "forks"})
                    elif name == 'procs_running':
                        self.add_stat("processes", long(tokens[1]), now, {"type": "running"})
                else:
                    cpu_suffix = name[3:]
                    if not cpu_suffix:
                        continue
                    for k, v in zip(self.CPU_FIELDS, tokens[1:]):
                        self.add_stat("cpu", long(v), now, {"instance": cpu_suffix, "type": k})

    def read_df_stats(self):
        now = int(time.time())
        with open('/proc/mounts') as f:
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens or len(tokens) != 6:
                    continue
                if not tokens[1].startswith('/'):
                    continue
                mount_target, mount_path, mount_filesystem = tokens[:3]
                try:
                    stats = os.statvfs(mount_path)
                    total_inodes = long(stats.f_files)
                    # Skip special filesystems
                    if not total_inodes:
                        continue
                    block_size = stats.f_bsize
                    self.add_stat("df", long(stats.f_bavail) * block_size, now,
                                  dict(target=mount_target, instance=mount_path, fs=mount_filesystem,
                                       type="bytes", description="free"))
                    self.add_stat("df", long(stats.f_blocks) * block_size, now,
                                  dict(target=mount_target, instance=mount_path, fs=mount_filesystem,
                                       type="bytes", description="total"))
                    self.add_stat("df", long(stats.f_favail), now,
                                  dict(target=mount_target, instance=mount_path, fs=mount_filesystem,
                                       type="inodes", description="free"))
                    self.add_stat("df", total_inodes, now,
                                  dict(target=mount_target, instance=mount_path, fs=mount_filesystem,
                                       type="inodes", description="total"))
                except OSError:
                    pass

    def read_interface_stats(self):
        now = int(time.time())
        with open('/proc/net/dev') as f:
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens or len(tokens) != 17:
                    continue
                if not tokens[0].endswith(':'):
                    continue
                name = tokens[0][:-1]
                self.add_stat("interface", long(tokens[1]), now, dict(instance=name, direction="rx", type="bytes"))
                self.add_stat("interface", long(tokens[2]), now, dict(instance=name, direction="rx", type="packets"))
                self.add_stat("interface", long(tokens[3]), now, dict(instance=name, direction="rx", type="errors"))
                self.add_stat("interface", long(tokens[4]), now, dict(instance=name, direction="rx", type="drops"))
                self.add_stat("interface", long(tokens[9]), now, dict(instance=name, direction="tx", type="bytes"))
                self.add_stat("interface", long(tokens[10]), now, dict(instance=name, direction="tx", type="packets"))
                self.add_stat("interface", long(tokens[11]), now, dict(instance=name, direction="tx", type="errors"))
                self.add_stat("interface", long(tokens[12]), now, dict(instance=name, direction="tx", type="drops"))

    def read_load_stats(self):
        now = int(time.time())
        with open('/proc/loadavg') as f:
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens or len(tokens) != 5:
                    continue
                self.add_stat("load", float(tokens[0]), now, dict(type="1m"))
                self.add_stat("load", float(tokens[1]), now, dict(type="5m"))
                self.add_stat("load", float(tokens[2]), now, dict(type="15m"))

    def read_memory_stats(self):
        now = int(time.time())
        with open('/proc/meminfo') as f:
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens or len(tokens) != 3 or tokens[2].lower() != 'kb':
                    continue
                name = tokens[0]
                if not name.endswith(":"):
                    continue
                name = name[:-1].lower()
                if name == "memtotal":
                    self.add_stat("memory", long(tokens[1]) * 1024, now, dict(type="total", description="bytes"))
                elif name == "memfree":
                    self.add_stat("memory", long(tokens[1]) * 1024, now, dict(type="free", description="bytes"))
                elif name == "memavailable":
                    self.add_stat("memory", long(tokens[1]) * 1024, now, dict(type="available", description="bytes"))
