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

import bucky.cfg as cfg


__host_trim__ = None


def _get_host_trim():
    global __host_trim__
    if __host_trim__ is not None:
        return __host_trim__
    host_trim = cfg.name_host_trim
    __host_trim__ = []
    for s in host_trim:
        s = list(reversed([p.strip() for p in s.split(".")]))
        __host_trim__.append(s)
    return __host_trim__


def hostname(host):
    host_trim = _get_host_trim()
    parts = host.split(".")
    parts = list(reversed([p.strip() for p in parts]))
    for s in host_trim:
        same = True
        for i, p in enumerate(s):
            if p != parts[i]:
                same = False
                break
        if same:
            parts = parts[len(s):]
            return parts
    return parts


def strip_duplicates(parts):
    ret = []
    for p in parts:
        if len(ret) == 0 or p != ret[-1]:
            ret.append(p)
    return ret


def statname(host, name):
    nameparts = name.split('.')
    parts = []
    if cfg.name_prefix:
        parts.append(cfg.name_prefix)
    if host:
        parts.extend(hostname(host))
    parts.extend(nameparts)
    if cfg.name_postfix:
        parts.append(cfg.name_postfix)
    if cfg.name_replace_char is not None:
        parts = [p.replace(".", cfg.name_replace_char) for p in parts]
    if cfg.name_strip_duplicates:
        parts = strip_duplicates(parts)
    return ".".join(parts)
