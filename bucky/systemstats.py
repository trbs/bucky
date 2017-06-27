
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
        self.metadata = {}
        if cfg.metadata:
            self.metadata.update(cfg.metadata)
        if cfg.system_stats_metadata:
            self.metadata.update(cfg.system_stats_metadata)
        self.interval = cfg.system_stats_interval
        self.ignored_filesystems = set()
        if cfg.system_stats_df_ignored:
            self.ignored_filesystems.update(cfg.system_stats_df_ignored)

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
            self.read_disk_stats()
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
            process_stats = {}
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens:
                    continue
                name = tokens[0]
                if not name.startswith('cpu'):
                    if name == 'ctxt':
                        process_stats['switches'] = long(tokens[1])
                    elif name == 'processes':
                        process_stats['forks'] = long(tokens[1])
                    elif name == 'procs_running':
                        process_stats['running'] = long(tokens[1])
                else:
                    cpu_suffix = name[3:]
                    if not cpu_suffix:
                        continue
                    cpu_stats = {k: v for k, v in zip(self.CPU_FIELDS, tokens[1:])}
                    self.add_stat("cpu", cpu_stats, now, {"instance": cpu_suffix})
            if process_stats:
                self.add_stat("processes", process_stats, now, metadata=None)

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
                if mount_filesystem in self.ignored_filesystems:
                    continue
                try:
                    stats = os.statvfs(mount_path)
                    total_inodes = long(stats.f_files)
                    # Skip special filesystems
                    if not total_inodes:
                        continue
                    block_size = stats.f_bsize
                    df_stats = {
                        'free_bytes': long(stats.f_bavail) * block_size,
                        'total_bytes': long(stats.f_blocks) * block_size,
                        'free_inodes': long(stats.f_favail),
                        'total_inodes': total_inodes
                    }
                    self.add_stat("df", df_stats, now,
                                  metadata=dict(target=mount_target, instance=mount_path, fs=mount_filesystem))
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
                interface_stats = {
                    'rx_bytes': long(tokens[1]),
                    'rx_packets': long(tokens[2]),
                    'rx_errors': long(tokens[3]),
                    'rx_dropped': long(tokens[4]),
                    'tx_bytes': long(tokens[9]),
                    'tx_packets': long(tokens[10]),
                    'tx_errors': long(tokens[11]),
                    'tx_dropped': long(tokens[12])
                }
                self.add_stat("interface", interface_stats, now, metadata=dict(instance=name))

    def read_load_stats(self):
        now = int(time.time())
        with open('/proc/loadavg') as f:
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens or len(tokens) != 5:
                    continue
                load_stats = {
                    'last_1m': float(tokens[0]),
                    'last_5m': float(tokens[1]),
                    'last_15m': float(tokens[2])
                }
                self.add_stat("load", load_stats, now, metadata=None)

    def read_memory_stats(self):
        now = int(time.time())
        with open('/proc/meminfo') as f:
            memory_stats = {}
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens or len(tokens) != 3 or tokens[2].lower() != 'kb':
                    continue
                name = tokens[0]
                if not name.endswith(":"):
                    continue
                name = name[:-1].lower()
                if name == "memtotal":
                    memory_stats['total_bytes'] = long(tokens[1]) * 1024
                elif name == "memfree":
                    memory_stats['free_bytes'] = long(tokens[1]) * 1024
                elif name == "memavailable":
                    memory_stats['available_bytes'] = long(tokens[1]) * 1024
            if memory_stats:
                self.add_stat("memory", memory_stats, now, metadata=None)

    def read_disk_stats(self):
        now = int(time.time())
        with open('/proc/diskstats') as f:
            for l in f.readlines():
                tokens = l.strip().split()
                if not tokens or len(tokens) != 14:
                    continue
                name = tokens[2]
                disk_stats = {
                    'read_ops': long(tokens[3]),
                    'read_merged': long(tokens[4]),
                    'read_sectors': long(tokens[5]),
                    'read_bytes': long(tokens[5]) * 512,
                    'read_time': long(tokens[6]),

                    'write_ops': long(tokens[7]),
                    'write_merged': long(tokens[8]),
                    'write_sectors': long(tokens[9]),
                    'write_bytes': long(tokens[9]) * 512,
                    'write_time': long(tokens[10]),

                    'in_progress': long(tokens[11]),
                    'io_time': long(tokens[12]),
                    'weighted_time': long(tokens[13])
                }
                self.add_stat("disk", disk_stats, now, metadata=dict(instance=name))
