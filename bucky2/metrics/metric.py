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

import time


class MetricValue(object):
    def __init__(self, name, value, now=None):
        self.name = name
        self.value = value
        self.time = now or time.time()

class Metric(object):
    def update(self, value):
        raise NotImplemented()
    
    def clear(self, value):
        raise NotImplemented()
    
    def metrics(slef):
        raise NotImplemented()
