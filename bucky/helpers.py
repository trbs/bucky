import os
import multiprocessing

import watchdog.observers
import watchdog.events


class SingleFileEventHandler(watchdog.events.FileSystemEventHandler):
    def __init__(self, path, flag):
        super(SingleFileEventHandler, self).__init__()
        self.path = path
        self.flag = flag

    def on_modified(self, event):
        if event.src_path == self.path.encode():
            self.flag.value = 1


class FileMonitor(object):
    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.flag = multiprocessing.Value('i', 0)
        self.event_handler = SingleFileEventHandler(self.path, self.flag)
        self.observer = watchdog.observers.Observer()
        self.observer.schedule(self.event_handler, os.path.dirname(self.path))
        self.observer.start()

    def modified(self):
        if self.flag.value:
            self.flag.value = 0
            return True
        return False

    def stop(self):
        self.observer.stop()
        self.observer.join()
