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

import copy
import logging
import os
import socket
import struct
import sys
import threading


log = logging.getLogger(__name__)


class CollectDError(Exception):
    def __init__(self, mesg):
        self.mesg = mesg
    def __str__(self):
        return self.mesg


class ConfigError(CollectDError):
    pass


class ProtocolError(CollectDError):
    pass


class BindError(CollectDError):
    pass


class ServerErrror(CollectDError):
    pass


class CPUConverter(object):
    PRIORITY = 0
    def __call__(self, sample):
        print sample
        return ["foo", "bar", "baz"]


class MemoryConverter(object):
    PRIORITY = 0
    def __call__(self, sample):
        return ["memory", sample["type_instance"]]


DEFAULT_CONVERTERS = {
    "cpu": CPUConverter(),
    "memory": MemoryConverter(),
}


class CollectDTypes(object):
    def __init__(self, types_db=None):
        self.types_db = types_db
        self.types = {}
        self.type_ranges = {}
        self._load_types()

    def get(self, name):
        t = self.types.get(name)
        if t is None:
            raise ProtocolError("Invalid type name: %s" % name)
        return t

    def _load_types(self):
        with open(self._fname()) as handle:
            for line in handle:
                if line.lstrip()[:1] == "#":
                    continue
                if not line.strip():
                    continue
                self._add_type_line(line)

    def _fname(self):
        if self.types_db is not None:
            return self.typesdb
        ret = "/usr/share/collectd/types.db"
        if os.path.exists(ret):
            return ret
        ret = "/usr/local/share/collectd/types.db"
        if os.path.exists(ret):
            return ret
        raise ConfigError("Unable to locate types.db")

    def _add_type_line(self, line):
        types = {
            "COUNTER": 0,
            "GAUGE": 1,
            "DERIVE": 2,
            "ABSOLUTE": 3
        }
        name, spec = line.split(None, 1)
        self.types[name] = []
        self.type_ranges[name] = {}
        vals = spec.split(", ")
        for val in vals:
            vname, vtype, minv, maxv = val.strip().split(":")
            vtype = types.get(vtype)
            if vtype is None:
                raise ValueError("Invalid value type: %s" % vtype)
            minv = None if minv == "U" else float(minv)
            maxv = None if maxv == "U" else float(maxv)
            self.types[name].append((vname, vtype))
            self.type_ranges[name][vname] = (minv, maxv)


class CollectDParser(object):
    def __init__(self, types_db=None):
        self.types = CollectDTypes(types_db=types_db)

    def parse(self, data):
        for sample in self.parse_samples(data):
            yield sample

    def parse_samples(self, data):
        types = {
            0x0000: self._parse_string("host"),
            0x0001: self._parse_time("time"),
            0x0008: self._parse_time_hires("time"),
            0x0002: self._parse_string("plugin"),
            0x0003: self._parse_string("plugin_instance"),
            0x0004: self._parse_string("type"),
            0x0005: self._parse_string("type_instance"),
            0x0006: None, # handle specially
            0x0007: self._parse_time("interval"),
            0x0009: self._parse_time_hires("interval")
        }
        sample = {}
        for (ptype, data) in self.parse_data(data):
            if ptype not in types:
                log.debug("Ignoring part type: 0x%02x" % ptype)
                continue
            if ptype != 0x0006:
                types[ptype](sample, data)
                continue
            for vname, vtype, val in self.parse_values(sample["type"], data):
                sample["value_name"] = vname
                sample["value_type"] = vtype
                sample["value"] = val
                yield copy.deepcopy(sample)

    def parse_data(self, data):
        types = set([
            0x0000, 0x0001, 0x0002, 0x0003, 0x0004,
            0x0005, 0x0006, 0x0007, 0x0008, 0x0009,
            0x0100, 0x0101, 0x0200, 0x0210
        ])
        while len(data) > 0:
            if len(data) < 4:
                raise ProtocolError("Truncated header.")
            (part_type, part_len) = struct.unpack("!HH", data[:4])
            data = data[4:]
            if part_type not in types:
                raise ProtocolError("Invalid part type: 0x%02x" % part_type)
            part_len -= 4 # includes four header bytes we just parsed
            if len(data) < part_len:
                raise ProtocolError("Truncated value.")
            part_data, data = data[:part_len], data[part_len:]
            yield (part_type, part_data)

    def parse_values(self, stype, data):
        types = {0: "!Q", 1: "<d", 2: "!q", 3: "!Q"}
        (nvals,) = struct.unpack("!H", data[:2])
        data = data[2:]
        if len(data) != 9 * nvals:
            raise ProtocolError("Invalid value structure length.")
        vtypes = self.types.get(stype)
        if nvals != len(vtypes):
            raise ProtocolError("Values different than types.db info.")
        for i in range(nvals):
            (vtype,) = struct.unpack("B", data[i])
            if vtype != vtypes[i][1]:
                raise ProtocolError("Type mismatch with types.db")
        data = data[nvals:]
        for i in range(nvals):
            vdata, data = data[:8], data[8:]
            (val,) = struct.unpack(types[vtypes[i][1]], vdata)
            yield vtypes[i][0], vtypes[i][1], val

    def _parse_string(self, name):
        def _parser(sample, data):
            if data[-1] != '\0':
                raise ProtocolError("Invalid string detected.")
            sample[name] = data[:-1]
        return _parser

    def _parse_time(self, name):
        def _parser(sample, data):
            if len(data) != 8:
                raise ProtocolError("Invalid time data length.")
            (val,) = struct.unpack("!Q", data)
            sample[name] = float(val)
        return _parser

    def _parse_time_hires(self, name):
        def _parser(sample, data):
            if len(data) != 8:
                raise ProtocolError("Invalid hires time data length.")
            (val,) = struct.unpack("!Q", data)
            sample[name] = val * (2 ** -30)
        return _parser


class CollectDConverter(object):
    def __init__(self, cfg):
        self.prefix = cfg.get("collectd_prefix")
        self.postfix = cfg.get("collectd_postfix")
        self.replace = cfg.get("collectd_replace", "_")
        self.strip_dupes = cfg.get("collectd_strip_duplicates", True)
        self.use_entry_points = cfg.get("collectd_use_entry_points", True)
        host_trim = cfg.get("collectd_host_trim", [])
        self.host_trim = []
        for s in host_trim:
            s = list(reversed(p.strip() for p in s.split(".")))
            self.strip.append(s)
        self.converters = dict(DEFAULT_CONVERTERS)
        self._load_converters(cfg)

    def convert(self, sample):
        stat = self.stat(sample)
        return stat, sample["value_type"], sample["value"], int(sample["time"])

    def stat(self, sample):
        parts = []
        if self.prefix:
            parts.append(self.prefix)
        parts.extend(self.hostname(sample.get("host", "")))
        handler = self.converters.get(sample["plugin"], self.default)
        parts.extend(handler(sample))
        if self.postfix:
            parts.append(self.postfix)
        if self.replace is not None:
            parts = [p.replace(".", self.replace) for p in parts]
        if self.strip_duplicates:
            parts = self.strip_duplicates(parts)
        return ".".join(parts)

    def hostname(self, host):
        parts = host.split(".")
        parts = list(reversed([p.strip() for p in parts]))
        for s in self.host_trim:
            same = True
            for i, p in enumerate(s):
                if p != parts[i]:
                    same = False
                    break
            if same:
                parts = parts[len(s):]
        return parts

    def default(self, sample):
        parts = []
        parts.append(sample["plugin"].strip())
        if sample.get("plugin_instance"):
            parts.append(sample["plugin_instance"].strip())
        stype = sample.get("type", "").strip()
        if stype and stype != "value":
            parts.append(stype)
        stypei = sample.get("type_instance", "").strip()
        if stypei:
            parts.append(stypei)
        vname = sample.get("value_name").strip()
        if vname and vname != "value":
            parts.append(vname)
        return parts

    def strip_duplicates(self, parts):
        ret = []
        for p in parts:
            if len(ret) == 0 or p != ret[-1]:
                ret.append(p)
        return ret

    def _load_converters(self, cfg):
        cfg_conv = cfg.get("collectd_converters", {})
        for conv in cfg_conv:
            self._add_converter(conv, cfg_conv[conv])
        if not cfg["collectd_use_entry_points"]:
            return
        import pkg_resources
        group = 'bucky.collectd.converters'
        for ep in pkg_resources.iter_entry_points(group):
            name, klass = ep.name, ep.load()
            self._add_converter(name, klass())

    def _add_converter(self, name, inst):
        if name not in self.converters:
            log.info("Converter: %s from %s" % (name, ep.module_name))
            self.converters[name] = klass()
            return
        kpriority = getattr(klass, "kpriority", 0)
        ipriority = getattr(self.converters[name], "PRIORITY", 0)
        if kpriority > ipriority:
            log.info("Replacing: %s" % name)
            log.info("Converter: %s from %s" % (name, ep.module_name))
            self.converters[name] = klass()
            return
        log.info("Ignoring: %s from %s" % (name, ep.module_name))


class CollectDServer(threading.Thread):
    def __init__(self, queue, cfg):
        super(CollectDServer, self).__init__()
        self.setDaemon(True)

        self.queue = queue
        self.parser = CollectDParser(cfg["collectd_types"])
        self.converter = CollectDConverter(cfg)
        self.sock = self.init_socket(cfg["collectd_ip"], cfg["collectd_port"])
        self.prev_samples = {}

    def init_socket(self, ip, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((ip, port))
            log.info("Opened collectd socket %s:%s" % (ip, port))
            return sock
        except Exception:
            log.error("Error opening collectd socket %s:%s." % (ip, port))
            sys.exit(1)

    def run(self):
        while True:
            data, addr = self.sock.recvfrom(65535)
            try:
                for sample in self.parser.parse(data):
                    name, vtype, val, time = self.converter.convert(sample)
                    if not name.strip():
                        continue
                    val = self.calculate(name, vtype, val, time)
                    if val is not None:
                        self.queue.put((name, val, time))
            except ProtocolError, e:
                log.error("Protocol error: %s" % e)

    def calculate(self, name, vtype, val, time):
        handlers = {
            0: self._calc_counter,  # counter
            1: lambda _name, v, _time: v,         # gauge
            2: self._calc_derive,  # derive
            3: self._calc_absolute  # absolute
        }
        if vtype not in handlers:
            log.error("Invalid value type %s for %s" % (vtype, name))
            return
        return handlers[vtype](name, val, time)

    def _calc_counter(self, name, val, time):
        # I need to figure out how to handle wrapping
        # Read: http://oss.oetiker.ch/rrdtool/tut/rrdtutorial.en.html
        # and then fix later
        if name not in self.prev_samples:
            self.prev_samples[name] = (val, time)
            return
        pval, ptime = self.prev_samples[name]
        self.prev_samples[name] = (val, time)
        if val < pval:
            return
        return (val - pval) / (time - ptime)

    def _calc_derive(self, name, val, time):
        # Like counter, I need to figure out wrapping
        if name not in self.prev_samples:
            self.prev_samples[name] = (val, time)
            return
        pval, ptime = self.prev_samples[name]
        self.prev_samples[name] = (val, time)
        return (val - pval) / (time - ptime)

    def _calc_absolute(self, name, val, time):
        if name not in self.prev_samples:
            self.prev_samples[name] = (val, time)
            return
        _pval, ptime = self.prev_samples[name]
        self.prev_samples[name] = (val, time)
        return val / (time - ptime)
