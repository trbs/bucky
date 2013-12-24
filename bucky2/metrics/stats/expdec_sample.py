# -*- coding: utf-8 -
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# Copyright 2011 Cloudant, Inc.

import heapq
import math
import random
import time

class ExpDecSample(object):
    """\
    An exponentially-decaying random sample of longs. Based
    on the implementation in Coda Hale's metrics library:
    
      https://github.com/codahale/metrics/blob/development/metrics-core/src/main/java/com/yammer/metrics/stats/ExponentiallyDecayingSample.java
    """
    
    RESCALE_THRESHOLD = 60 * 60 * 1000000000
    
    def __init__(self, reservoir_size, alpha):
        self.rsize = reservoir_size
        self.alpha = alpha
        self.values = []
        self.count = 0
        self.start_time = self.tick()
        self.next_rescale = self.start_time + self.RESCALE_THRESHOLD
    
    def clear(self):
        self.count = 0
        self.start_time = self.tick()
        self.next_rescale = self.start_time + self.RESCALE_THRESHOLD
    
    def size(self):
        return int(min(self.rsize, self.count))
    
    def update(self, val, when=None):
        if when is None:
            when = self.tick()
        priority = self.weight(when - self.start_time) / random.random()
        self.count += 1
        if self.count <= self.rsize:
            heapq.heappush(self.values, (priority, val))
        else:
            if priority > self.values[0][0]:
                heapq.heapreplace(self.values, (priority, val))
        now = self.tick()
        if now >= self.next_rescale:
            self.rescale(now, self.next_rescale)

    def rescale(self, now, next):
        # See the comment in the original Java implementation.
        self.next_rescale = now + self.RESCALE_THRESHOLD
        old_start = self.start_time
        self.start_time = self.tick()
        newvals = []
        factor = math.exp(-self.alpha * (self.start_time - old_start))
        for k, v in self.values:
            newvals.append((k * factor, v))
        self.values = newvals

    def tick(self):
        return long(time.time() * 1000000000.0)

    def weight(self, t):
        return math.exp(self.alpha * t)

    def get_values(self):
        return [v for (_, v) in self.values]
