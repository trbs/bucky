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

import os
import six
import copy
import struct
import logging

import hmac
from hashlib import sha1
from hashlib import sha256
from Crypto.Cipher import AES

from bucky.errors import ConfigError, ProtocolError
from bucky.udpserver import UDPServer
from bucky.helpers import FileMonitor

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
                "/usr/local/share/collectd/types.db",
            ])
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
            log.info("Loaded collectd types from %s", types_db)

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
            0x0006: None,  # handle specially
            0x0007: self._parse_time("interval"),
            0x0009: self._parse_time_hires("interval")
        }
        sample = {}
        for (ptype, data) in self.parse_data(data):
            if ptype not in types:
                log.debug("Ignoring part type: 0x%02x", ptype)
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
            part_len -= 4  # includes four header bytes we just parsed
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
            if six.PY3:
                vtype = data[i]
            else:
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
            if six.PY3:
                data = data.decode()
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


class CollectDCrypto(object):
    def __init__(self, cfg):
        sec_level = cfg.collectd_security_level
        if sec_level in ("sign", "SIGN", "Sign", 1):
            self.sec_level = 1
        elif sec_level in ("encrypt", "ENCRYPT", "Encrypt", 2):
            self.sec_level = 2
        else:
            self.sec_level = 0
        self.auth_file = cfg.collectd_auth_file
        self.auth_db = {}
        self.cfg_mon = None
        if self.auth_file:
            self.load_auth_file()
            self.cfg_mon = FileMonitor(self.auth_file)
        if self.sec_level:
            if not self.auth_file:
                raise ConfigError("Collectd security level configured but no "
                                  "auth file specified in configuration")
            if not self.auth_db:
                raise ConfigError("Collectd security level configured but no "
                                  "user/passwd entries loaded from auth file")

    def load_auth_file(self):
        try:
            f = open(self.auth_file)
        except IOError as exc:
            raise ConfigError("Unable to load collectd's auth file: %r", exc)
        self.auth_db.clear()
        for line in f:
            line = line.strip()
            if not line or line[0] == "#":
                continue
            user, passwd = line.split(":", 1)
            user = user.strip()
            passwd = passwd.strip()
            if not user or not passwd:
                log.warning("Found line with missing user or password")
                continue
            if user in self.auth_db:
                log.warning("Found multiple entries for single user")
            self.auth_db[user] = passwd
        f.close()
        log.info("Loaded collectd's auth file from %s", self.auth_file)

    def parse(self, data):
        if len(data) < 4:
            raise ProtocolError("Truncated header.")
        part_type, part_len = struct.unpack("!HH", data[:4])
        sec_level = {0x0200: 1, 0x0210: 2}.get(part_type, 0)
        if sec_level < self.sec_level:
            raise ProtocolError("Packet has lower security level than allowed")
        if not sec_level:
            return data
        if sec_level == 1 and not self.sec_level:
            return data[part_len:]
        data = data[4:]
        part_len -= 4
        if len(data) < part_len:
            raise ProtocolError("Truncated part payload.")
        if self.cfg_mon is not None and self.cfg_mon.modified():
            log.info("Collectd authfile modified, reloading")
            self.load_auth_file()
        if sec_level == 1:
            return self.parse_signed(part_len, data)
        if sec_level == 2:
            return self.parse_encrypted(part_len, data)

    def parse_signed(self, part_len, data):
        if part_len <= 32:
            raise ProtocolError("Truncated signed part.")
        sig, data = data[:32], data[32:]
        uname_len = part_len - 32
        uname = data[:uname_len].decode()
        if uname not in self.auth_db:
            raise ProtocolError("Signed packet, unknown user '%s'" % uname)
        password = self.auth_db[uname].encode()
        sig2 = hmac.new(password, msg=data, digestmod=sha256).digest()
        if not self._hashes_match(sig, sig2):
            raise ProtocolError("Bad signature from user '%s'" % uname)
        data = data[uname_len:]
        return data

    def parse_encrypted(self, part_len, data):
        if part_len != len(data):
            raise ProtocolError("Enc pkt size disaggrees with header.")
        if len(data) <= 38:
            raise ProtocolError("Truncated encrypted part.")
        uname_len, data = struct.unpack("!H", data[:2])[0], data[2:]
        if len(data) <= uname_len + 36:
            raise ProtocolError("Truncated encrypted part.")
        uname, data = data[:uname_len].decode(), data[uname_len:]
        if uname not in self.auth_db:
            raise ProtocolError("Couldn't decrypt, unknown user '%s'" % uname)
        iv, data = data[:16], data[16:]
        password = self.auth_db[uname].encode()
        key = sha256(password).digest()
        pad_bytes = 16 - (len(data) % 16)
        data += b'\0' * pad_bytes
        data = AES.new(key, IV=iv, mode=AES.MODE_OFB).decrypt(data)
        data = data[:-pad_bytes]
        tag, data = data[:20], data[20:]
        tag2 = sha1(data).digest()
        if not self._hashes_match(tag, tag2):
            raise ProtocolError("Bad checksum on enc pkt for '%s'" % uname)
        return data

    def _hashes_match(self, a, b):
        """Constant time comparison of bytes for py3, strings for py2"""
        if len(a) != len(b):
            return False
        diff = 0
        if six.PY2:
            a = bytearray(a)
            b = bytearray(b)
        for x, y in zip(a, b):
            diff |= x ^ y
        return not diff


class CollectDConverter(object):
    def __init__(self, cfg):
        self.converters = dict(DEFAULT_CONVERTERS)
        self._load_converters(cfg)

    def convert(self, sample):
        default = self.converters["_default"]
        handler = self.converters.get(sample["plugin"], default)
        try:
            name_parts = handler(sample)
            if name_parts is None:
                return  # treat None as "ignore sample"
            name = '.'.join(name_parts)
        except:
            log.exception("Exception in sample handler  %s (%s):", sample["plugin"], handler)
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
            log.info("Converter: %s from %s", name, source)
            self.converters[name] = inst
            return
        kpriority = getattr(inst, "PRIORITY", 0)
        ipriority = getattr(self.converters[name], "PRIORITY", 0)
        if kpriority > ipriority:
            log.info("Replacing: %s", name)
            log.info("Converter: %s from %s", name, source)
            self.converters[name] = inst
            return
        log.info("Ignoring: %s (%s) from %s (priority: %s vs %s)",
                 name, inst, source, kpriority, ipriority)


class CollectDServer(UDPServer):
    def __init__(self, queue, cfg):
        super(CollectDServer, self).__init__(cfg.collectd_ip, cfg.collectd_port)
        self.queue = queue
        self.crypto = CollectDCrypto(cfg)
        self.parser = CollectDParser(cfg.collectd_types)
        self.converter = CollectDConverter(cfg)
        self.prev_samples = {}
        self.last_sample = None

    def handle(self, data, addr):
        try:
            data = self.crypto.parse(data)
        except ProtocolError as e:
            log.error("Protocol error in CollectDCrypto: %s", e)
            return True
        try:
            for sample in self.parser.parse(data):
                self.last_sample = sample
                stype = sample["type"]
                vname = sample["value_name"]
                sample = self.converter.convert(sample)
                if sample is None:
                    continue
                host, name, vtype, val, time = sample
                if not name.strip():
                    continue
                val = self.calculate(host, name, vtype, val, time)
                val = self.check_range(stype, vname, val)
                if val is not None:
                    self.queue.put((host, name, val, time))
        except ProtocolError as e:
            log.error("Protocol error: %s", e)
            if self.last_sample is not None:
                log.info("Last sample: %s", self.last_sample)
        return True

    def check_range(self, stype, vname, val):
        if val is None:
            return
        try:
            vmin, vmax = self.parser.types.type_ranges[stype][vname]
        except KeyError:
            log.error("Couldn't find vmin, vmax in CollectDTypes")
            return val
        if vmin is not None and val < vmin:
            log.debug("Invalid value %s (<%s) for %s", val, vmin, vname)
            log.debug("Last sample: %s", self.last_sample)
            return
        if vmax is not None and val > vmax:
            log.debug("Invalid value %s (>%s) for %s", val, vmax, vname)
            log.debug("Last sample: %s", self.last_sample)
            return
        return val

    def calculate(self, host, name, vtype, val, time):
        handlers = {
            0: self._calc_counter,  # counter
            1: lambda _host, _name, v, _time: v,  # gauge
            2: self._calc_derive,  # derive
            3: self._calc_absolute  # absolute
        }
        if vtype not in handlers:
            log.error("Invalid value type %s for %s", vtype, name)
            log.info("Last sample: %s", self.last_sample)
            return
        return handlers[vtype](host, name, val, time)

    def _calc_counter(self, host, name, val, time):
        key = (host, name)
        if key not in self.prev_samples:
            self.prev_samples[key] = (val, time)
            return
        pval, ptime = self.prev_samples[key]
        self.prev_samples[key] = (val, time)
        if time <= ptime:
            log.error("Invalid COUNTER update for: %s:%s" % key)
            log.info("Last sample: %s", self.last_sample)
            return
        if val < pval:
            # this is supposed to handle counter wrap around
            # see https://collectd.org/wiki/index.php/Data_source
            log.debug("COUNTER wrap-around for: %s:%s (%s -> %s)",
                      host, name, pval, val)
            if pval < 0x100000000:
                val += 0x100000000  # 2**32
            else:
                val += 0x10000000000000000  # 2**64
        return float(val - pval) / (time - ptime)

    def _calc_derive(self, host, name, val, time):
        key = (host, name)
        if key not in self.prev_samples:
            self.prev_samples[key] = (val, time)
            return
        pval, ptime = self.prev_samples[key]
        self.prev_samples[key] = (val, time)
        if time <= ptime:
            log.debug("Invalid DERIVE update for: %s:%s" % key)
            log.debug("Last sample: %s", self.last_sample)
            return
        return float(val - pval) / (time - ptime)

    def _calc_absolute(self, host, name, val, time):
        key = (host, name)
        if key not in self.prev_samples:
            self.prev_samples[key] = (val, time)
            return
        _pval, ptime = self.prev_samples[key]
        self.prev_samples[key] = (val, time)
        if time <= ptime:
            log.error("Invalid ABSOLUTE update for: %s:%s" % key)
            log.info("Last sample: %s", self.last_sample)
            return
        return float(val) / (time - ptime)
