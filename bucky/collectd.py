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
import struct

from bucky.errors import ConfigError, ProtocolError
from bucky.udpserver import UDPServer

log = logging.getLogger(__name__)


class CPUConverter(object):
    PRIORITY = -1
    def __call__(self, sample):
        return ["cpu", sample["plugin_instance"], sample["type_instance"]]


class InterfaceConverter(object):
    PRIORITY = -1
    def __call__(self, sample):
        return filter(None, [
            "interface",
            sample.get("plugin_instance", ""),
            sample.get("type_instance", ""),
            sample["type"],
            sample["value_name"]
        ])


class MemoryConverter(object):
    PRIORITY = -1
    def __call__(self, sample):
        return ["memory", sample["type_instance"]]


class DefaultConverter(object):
    PRIORITY = -1
    def __call__(self, sample):
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


DEFAULT_CONVERTERS = {
    "cpu": CPUConverter(),
    "interface": InterfaceConverter(),
    "memory": MemoryConverter(),
    "_default": DefaultConverter(),
}


class CollectDTypes(object):
    def __init__(self, types_dbs=[]):
        self.types = {}
        self.type_ranges = {}
        if not types_dbs:
            types_dbs = filter(os.path.exists, [
                "/usr/share/collectd/types.db",
                "/usr/local/share/collectd/types.db" ])
            if not types_dbs:
                raise ConfigError("Unable to locate types.db")
        self.types_dbs = types_dbs
        self._load_types()

    def get(self, name):
        t = self.types.get(name)
        if t is None:
            raise ProtocolError("Invalid type name: %s" % name)
        return t

    def _load_types(self):
        for types_db in self.types_dbs:
            with open(types_db) as handle:
                for line in handle:
                    if line.lstrip()[:1] == "#":
                        continue
                    if not line.strip():
                        continue
                    self._add_type_line(line)
            log.info("Loaded collectd types from %s" % types_db)

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
    def __init__(self, types_dbs=[]):
        self.types = CollectDTypes(types_dbs=types_dbs)

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
        self.converters = dict(DEFAULT_CONVERTERS)
        self._load_converters(cfg)

    def convert(self, sample):
        default = self.converters["_default"]
        handler = self.converters.get(sample["plugin"], default)
        try:
            name = '.'.join(handler(sample))
            if name is None:
                return # treat None as "ignore sample"
        except:
            log.exception("Exception in sample handler  %s (%s):" % (
                sample["plugin"], handler))
            return
        host = sample.get("host", "")
        return (
            host,
            name,
            sample["value_type"],
            sample["value"],
            int(sample["time"])
        )

    def _load_converters(self, cfg):
        cfg_conv = cfg.collectd_converters
        for conv in cfg_conv:
            self._add_converter(conv, cfg_conv[conv], source="config")
        if not cfg.collectd_use_entry_points:
            return
        import pkg_resources
        group = 'bucky.collectd.converters'
        for ep in pkg_resources.iter_entry_points(group):
            name, klass = ep.name, ep.load()
            self._add_converter(name, klass, source=ep.module_name)

    def _add_converter(self, name, inst, source="unknown"):
        if name not in self.converters:
            log.info("Converter: %s from %s" % (name, source))
            self.converters[name] = inst
            return
        kpriority = getattr(inst, "PRIORITY", 0)
        ipriority = getattr(self.converters[name], "PRIORITY", 0)
        if kpriority > ipriority:
            log.info("Replacing: %s" % name)
            log.info("Converter: %s from %s" % (name, source))
            self.converters[name] = inst
            return
        log.info( "Ignoring: %s (%s) from %s (priority: %s vs %s)"
            % (name, inst, source, kpriority, ipriority) )


class CollectDServer(UDPServer):
    def __init__(self, queue, cfg):
        super(CollectDServer, self).__init__(cfg.collectd_ip, cfg.collectd_port)
        self.queue = queue
        self.parser = CollectDParser(cfg.collectd_types)
        self.converter = CollectDConverter(cfg)
        self.prev_samples = {}
        self.last_sample = None

    def handle(self, data, addr):
        try:
            for sample in self.parser.parse(data):
                self.last_sample = sample
                sample = self.converter.convert(sample)
                if sample is None:
                    continue
                host, name, vtype, val, time = sample
                if not name.strip():
                    continue
                val = self.calculate(name, vtype, val, time)
                if val is not None:
                    self.queue.put((host, name, val, time))
        except ProtocolError, e:
            log.error("Protocol error: %s" % e)
            if self.last_sample is not None:
                log.info("Last sample: %s" % self.last_sample)
        return True

    def calculate(self, name, vtype, val, time):
        handlers = {
            0: self._calc_counter,  # counter
            1: lambda _name, v, _time: v,         # gauge
            2: self._calc_derive,  # derive
            3: self._calc_absolute  # absolute
        }
        if vtype not in handlers:
            log.error("Invalid value type %s for %s" % (vtype, name))
            log.info("Last sample: %s" % self.last_sample)
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
        if val < pval or time <= ptime:
            log.error("Invalid COUNTER update for: %s" % name)
            log.info("Last sample: %s" % self.last_sample)
            return
        return float(val - pval) / (time - ptime)

    def _calc_derive(self, name, val, time):
        # Like counter, I need to figure out wrapping
        if name not in self.prev_samples:
            self.prev_samples[name] = (val, time)
            return
        pval, ptime = self.prev_samples[name]
        self.prev_samples[name] = (val, time)
        if time <= ptime:
            log.debug("Invalid DERIVE update for: %s" % name)
            log.debug("Last sample: %s" % self.last_sample)
            return
        return float(val - pval) / (time - ptime)

    def _calc_absolute(self, name, val, time):
        if name not in self.prev_samples:
            self.prev_samples[name] = (val, time)
            return
        _pval, ptime = self.prev_samples[name]
        self.prev_samples[name] = (val, time)
        if time <= ptime:
            log.error("Invalid ABSOLUTE update for: %s" % name)
            log.info("Last sample: %s" % self.last_sample)
            return
        return float(val) / (time - ptime)
