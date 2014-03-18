import time
import random
import multiprocessing
from functools import wraps

try:
    import queue
except ImportError:
    import Queue as queue

import t
import bucky.processor
import bucky.cfg as cfg
cfg.debug = True


def processor(func):
    @wraps(func)
    def run():
        inq = multiprocessing.Queue()
        outq = multiprocessing.Queue()
        proc = bucky.processor.CustomProcessor(inq, outq, cfg)
        proc.start()
        func(inq, outq, proc)
        inq.put(None)
        dead = False
        for i in range(5):
            if not proc.is_alive():
                dead = True
                break
            time.sleep(0.1)
        if not dead:
            raise RuntimeError("Server didn't die.")
    return run


def send_get_data(indata, inq, outq):
    for sample in indata:
        inq.put(sample)
    while True:
        try:
            sample = outq.get(True, 1)
        except queue.Empty:
            break
        yield sample


def identity(host, name, val, time):
    return host, name, val, time


@t.set_cfg("processor", identity)
@processor
def test_start_stop(inq, outq, proc):
    assert proc.is_alive(), "Processor not alive."
    inq.put(None)
    time.sleep(0.5)
    assert not proc.is_alive(), "Processor not killed by putting None in queue"


@t.set_cfg("processor", identity)
@processor
def test_plumbing(inq, outq, proc):
    data = []
    times = 100
    for i in range(times):
        host = "tests.host-%d" % i
        name = "test-plumbing-%d" % i
        value = i
        timestamp = int(time.time() + i)
        data.append((host, name, value, timestamp))
    i = 0
    for sample in send_get_data(data, inq, outq):
        t.eq(sample, data[i])
        i += 1
    t.eq(i, times)


def filter_even(host, name, val, timestamp):
    if not val % 2:
        return None
    return host, name, val, timestamp


@t.set_cfg("processor", filter_even)
@processor
def test_filter(inq, outq, proc):
    data = []
    times = 100
    for i in range(times):
        host = "tests.host-%d" % i
        name = "test-filter-%d" % i
        timestamp = int(time.time() + i)
        data.append((host, name, 0, timestamp))
        data.append((host, name, 1, timestamp))
    i = 0
    for sample in send_get_data(data, inq, outq):
        t.eq(sample[2], 1)
        i += 1
    t.eq(i, times)
