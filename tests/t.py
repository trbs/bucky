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

import Queue
import time
import multiprocessing

import bucky.cfg as cfg
cfg.debug = True


class set_cfg(object):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __call__(self, func):
        def run(*args, **kwargs):
            curr = getattr(cfg, self.name)
            try:
                setattr(cfg, self.name, self.value)
                func(*args, **kwargs)
            finally:
                setattr(cfg, self.name, curr)
        run.func_name = func.func_name
        return run


class udp_srv(object):
    def __init__(self, stype):
        self.stype = stype

    def __call__(self, func):
        def run():
            q = multiprocessing.Queue()
            s = self.stype(q, cfg)
            s.start()
            try:
                func(q, s)
            finally:
                s.close()
                if not self.closed(s):
                    raise RuntimeError("Server didn't die.")
        run.func_name = func.func_name
        return run

    def closed(self, s):
        for i in range(5):
            if not s.is_alive():
                return True
            time.sleep(0.1)
        return False


def same_stat(host, name, value, stat):
    eq(name, stat[1])
    eq(value, stat[2])
    gt(stat[3], 0)


def eq(a, b):
    assert a == b, "%r != %r" % (a, b)

def ne(a, b):
    assert a != b, "%r == %r" % (a, b)

def lt(a, b):
    assert a < b, "%r >= %r" % (a, b)

def gt(a, b):
    assert a > b, "%r <= %r" % (a, b)

def isin(a, b):
    assert a in b, "%r is not in %r" % (a, b)

def isnotin(a, b):
    assert a not in b, "%r is in %r" % (a, b)

def has(a, b):
    assert hasattr(a, b), "%r has no attribute %r" % (a, b)

def hasnot(a, b):
    assert not hasattr(a, b), "%r has an attribute %r" % (a, b)

def istype(a, b):
    assert isinstance(a, b), "%r is not an instance of %r" % (a, b)

def raises(exctype, func, *args, **kwargs):
    try:
        func(*args, **kwargs)
    except exctype:
        return
    func_name = getattr(func, "func_name", "<builtin_function>")
    raise AssertionError("Function %s did not raise %s" % (
            func_name, exctype.__name__))

