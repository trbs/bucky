

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
        err = 0
        while True:
            start_timestamp = time.time()
            if not self.collect():
                err = min(err + 1, 2)
            else:
                err = 0
            stop_timestamp = time.time()
            sleep_time = (err + 1) * self.interval - (stop_timestamp - start_timestamp)
            if sleep_time > 0.1:
                time.sleep(sleep_time)

    def collect(self):
        raise NotImplementedError()

    def add_stat(self, name, value, timestamp, **metadata):
        if metadata:
            if self.metadata:
                metadata.update(self.metadata)
        else:
            metadata = self.metadata
        if metadata:
            self.queue.put((None, name, value, timestamp, metadata))
        else:
            self.queue.put((None, name, value, timestamp))

    def merge_dicts(self, *dicts):
        ret = {}
        for d in dicts:
            if d:
                ret.update(d)
        return ret
