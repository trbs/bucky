#!/usr/bin/env python

import multiprocessing
import bucky.statsd
import time
import timeit

l10 = range(10)
l100 = range(100)
l1000 = range(1000)

# try:
#     import queue
# except ImportError:
#     import Queue as queue

queue = multiprocessing.Queue()

handler = bucky.statsd.StatsDHandler(queue, bucky.cfg)


def fill_and_compute_timers(handler):
    # Fill timers
    for x in l100:  # timer name
        for y in l1000:  # timer value, using random value is not good idea there
            handler.handle_timer("timer-%s" % (x), [y])

    # Compute metrics
    stime = int(time.time())
    handler.enqueue_timers(stime)

    # Clear queue
    while not queue.empty():
        queue.get()


# Warmup
print("Warmup")
for i in l10:
    fill_and_compute_timers(handler)

print("Test")
trun = timeit.timeit('fill_and_compute_timers(handler)',
                     'from __main__ import fill_and_compute_timers, handler',
                     number=100)

print("Result:", trun)

queue.close
