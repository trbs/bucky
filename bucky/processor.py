import logging
import multiprocessing

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(title):
        pass

try:
    import queue
except ImportError:
    import Queue as queue


log = logging.getLogger(__name__)


class Processor(multiprocessing.Process):
    def __init__(self, in_queue, out_queue):
        super(Processor, self).__init__()
        self.daemon = True
        self.in_queue = in_queue
        self.out_queue = out_queue

    def run(self):
        setproctitle("bucky: %s" % self.__class__.__name__)
        while True:
            try:
                sample = self.in_queue.get(True, 1)
                if not sample:
                    break
            except queue.Empty:
                pass
            else:
                try:
                    sample = self.process(*sample)
                except Exception as exc:
                    log.error("Error processing sample %s: %r", sample, exc)
                    # drop or forward? maybe make it configurable
                if sample is not None:
                    self.out_queue.put(sample)

    def process(self, host, name, val, time):
        raise NotImplementedError


class CustomProcessor(Processor):
    def __init__(self, function, in_queue, out_queue):
        super(CustomProcessor, self).__init__(in_queue, out_queue)
        self.function = function

    def process(self, host, name, val, time):
        return self.function(host, name, val, time)
