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

import math

from bucky.metrics.metric import Metric, MetricValue as MV
from bucky.metrics.stats.expdec_sample import ExpDecSample
from bucky.metrics.stats.usample import UniformSample


class Histogram(Metric):
    def __init__(self, name, biased=True, percentiles=None):
        self.name = name
        if biased:
            self.sample = ExpDecSample(1028, 0.015)
        else:
            self.sample = UniformSample(1028)
        self.percentiles = self._fmt(percentiles or (75, 85, 90, 95, 99, 99.9))
        self.count = 0
        self.sum = 0
        self.minv = None
        self.maxv = None
        self.variance = (-1.0, 0.0)

    def clear(self):
        self.sample.clear()
        self.count = 0
        self.sum = 0
        self.minv = None
        self.maxv = None
        self.variance = (-1.0, 0.0)

    def update(self, value):
        self.count += 1
        self.sum += value
        self.sample.update(value)
        if self.minv is None or value < self.minv:
            self.minv = value
        if self.maxv is None or value > self.maxv:
            self.maxv = value
        self._update_variance(value)

    def metrics(self):
        ret = []
        ret.append(MV("%s.count" % self.name, self.count))
        ret.append(MV("%s.sum" % self.name, self.sum))
        ret.append(MV("%s.min" % self.name, self.minv))
        ret.append(MV("%s.max" % self.name, self.maxv))
        if self.count > 0:
            ret.append(MV("%s.mean" % self.name, self.sum / self.count))
            ret.append(MV("%s.stddev" % self.name, self._stddev()))
            for disp, val in self._percentiles():
                name = "%s.%s" % (self.name, disp)
                ret.append(MV(name, val))
        return ret

    def _stddev(self):
        if self.count <= 1:
            return 0.0
        return math.sqrt(self.variance[1] / (float(self.count) - 1.0))

    def _update_variance(self, value):
        oldm, olds = self.variance
        if oldm == -1.0:
            self.variance = (value, 0.0)
            return
        newm = oldm + ((value - oldm) / self.count)
        news = olds + ((value - oldm) * (value - newm))
        self.variance = (newm, news)
    
    def _percentiles(self):
        values = self.sample.values()
        values.sort()
        ret = []
        for (p, d) in self.percentiles:
            pos = p * len(values + 1)
            if pos < 1:
                ret.append((d, values[0]))
            elif pos >= len(values):
                ret.append((d, values[-1]))
            else:
                lower, upper = values[int(pos-1)], values[int(pos)]
                percentile = lower + ((pos - math.floor(pos)) * (upper - lower))
                ret.append((d, percentile))
        return ret

    def _fmt(self, percentiles):
        ret = []
        for p in percentiles:
            d = "%0.1f" % p
            if d.endswith(".0"):
                d = p[:-2]
            d = "perc_%s" % d.replace(".", "_")
            ret.append((p, d))
