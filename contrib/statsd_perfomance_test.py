#!/usr/bin/env python

from __future__ import print_function

import time
import timeit
import platform
import multiprocessing

import bucky.statsd

l10 = range(10)
l100 = range(100)
l1000 = range(1000)

# try:
#     import queue
# except ImportError:
#     import Queue as queue

queue = multiprocessing.Queue()

handler = bucky.statsd.StatsDServer(queue, bucky.cfg)


def fill_and_compute_timers(handler):
    # Fill timers
    for x in l100:  # timer name
        for y in l1000:  # timer value, using random value is not good idea there
            handler.handle_timer(("timer-%s" % (x,), ()), [y])

    # Compute metrics
    stime = int(time.time())
    handler.enqueue_timers(stime)

    # Clear queue
    while not queue.empty():
        queue.get()


def line_parsing_stress(handler):
    for x in l100:  # name
        for y in l1000:  # value
            handler.handle_line("gauge-%s:%s|g" % (x, y))
        handler.tick()

    # Clear queue
    while not queue.empty():
        queue.get()


# Warmup
print("Warmup")
for i in l10:
    fill_and_compute_timers(handler)
    line_parsing_stress(handler)

print("Test")
trun = timeit.timeit('fill_and_compute_timers(handler)',
                     'from __main__ import fill_and_compute_timers, handler',
                     number=100)
print("Result:", trun)
trun = timeit.timeit('line_parsing_stress(handler)',
                     'from __main__ import line_parsing_stress, handler',
                     number=100)
print("Result:", trun)

if platform.system() in ("Darwin", ):
    qsize = 0
    while not queue.empty():
        qsize += 1
        queue.get()
else:
    qsize = queue.qsize()

if qsize:
    print("Queue did not drain properly, left:", qsize)

queue.cancel_join_thread()
