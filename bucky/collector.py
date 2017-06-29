

import time
import multiprocessing


try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(title):
        pass


class StatsCollector(multiprocessing.Process):
    def __init__(self, queue):
        super(StatsCollector, self).__init__()
        self.queue = queue

    def close(self):
        pass

    def run(self):
        setproctitle("bucky: %s" % self.__class__.__name__)
        interval = self.interval
        while True:
            start_timestamp = time.time()
            interval = self.interval if self.collect() else interval + interval
            stop_timestamp = time.time()
            interval = min(interval, 300)
            interval = interval - (stop_timestamp - start_timestamp)
            if interval > 0.1:
                time.sleep(interval)

    def collect(self):
        raise NotImplementedError()

    def add_stat(self, name, value, timestamp, **metadata):
        if metadata:
            if self.metadata:
                metadata.update(self.metadata)
        else:
            metadata = self.metadata
        if metadata:
            metadata_tuple = tuple((k, metadata[k]) for k in sorted(metadata.keys()))
            self.queue.put((None, name, value, timestamp, metadata_tuple))
        else:
            self.queue.put((None, name, value, timestamp))

    def merge_dicts(self, *dicts):
        ret = {}
        for d in dicts:
            if d:
                ret.update(d)
        return ret
