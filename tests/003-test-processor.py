import time
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
            proc.terminate()
    return run


def get_simple_data(times=100):
    data = []
    for i in range(times):
        host = "tests.host-%d" % i
        name = "metric-%d" % i
        value = i
        timestamp = int(time.time() + i)
        data.append((host, name, value, timestamp))
    return data


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
    t.eq(proc.is_alive(), True)
    inq.put(None)
    time.sleep(0.5)
    t.eq(proc.is_alive(), False)


@t.set_cfg("processor", identity)
@processor
def test_plumbing(inq, outq, proc):
    data = get_simple_data(100)
    i = 0
    for sample in send_get_data(data, inq, outq):
        t.eq(sample, data[i])
        i += 1
    t.eq(i, 100)


def filter_even(host, name, val, timestamp):
    if not val % 2:
        return None
    return host, name, val, timestamp


@t.set_cfg("processor", filter_even)
@processor
def test_filter(inq, outq, proc):
    data = get_simple_data(100)
    i = 0
    for sample in send_get_data(data, inq, outq):
        t.eq(sample[2] % 2, 1)
        i += 1
    t.eq(i, 50)


def raise_error(host, name, val, timestamp):
    raise Exception()


@t.set_cfg("processor", raise_error)
@processor
def test_function_error(inq, outq, proc):
    data = get_simple_data(100)
    i = 0
    for sample in send_get_data(data, inq, outq):
        t.eq(sample, data[i])
        i += 1
    t.eq(proc.is_alive(), True)
    t.eq(i, 100)


@t.set_cfg("processor", raise_error)
@t.set_cfg("processor_drop_on_error", True)
@processor
def test_function_error_drop(inq, outq, proc):
    data = get_simple_data(100)
    samples = list(send_get_data(data, inq, outq))
    t.eq(proc.is_alive(), True)
    t.eq(len(samples), 0)
