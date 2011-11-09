
import copy
import logging
import os
import socket
import struct


log = logging.getLogger(__name__)


class CollectDError(Exception):
    def __init__(self, mesg):
        self.mesg = mesg
    def __str__(self):
        return self.mesg


class ProtocolError(CollectDError):
    pass


class BindError(CollectDError):
    pass


class ServerErrror(CollectDError):
    pass


class CollectDTypes(object):
    def __init__(self):
        self.types = {}
        self.type_ranges = {}
        self._load_types()

    def get(self, name):
        t = self.types.get(name)
        if t is None:
            raise ProtocolError("Invalid type name: %s" % name)
        return t

    def _load_types(self):
        fname = os.path.join(os.path.dirname(__file__), "types.db")
        with open(fname) as handle:
            for line in handle:
                if line.lstrip()[:1] == "#":
                    continue
                if not line.strip():
                    continue
                self._add_type_line(line)

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
    def __init__(self):
        self.types = CollectDTypes()

    def parse(self, data):
        for mesg in self.parse_messages(data):
            yield mesg

    def parse_messages(self, data):
        types = {
            0x0000: self._parse_string("host"),
            0x0001: self._parse_time("time"),
            0x0008: self._parse_time_hires("time"),
            0x0002: self._parse_string("plugin"),
            0x0003: self._parse_string("plugin_instance"),
            0x0004: self._parse_string("type"),
            0x0005: self._parse_string("type_instance"),
            0x0006: self._parse_values("values"),
            0x0007: self._parse_time("interval"),
            0x0009: self._parse_time_hires("interval")
        }
        mesg = {}
        for (ptype, data) in self.parse_data(data):
            if ptype not in types:
                log.debug("Ignoring part type: 0x%02x" % ptype)
                continue
            types[ptype](mesg, data)
            if ptype == 0x0006:
                yield copy.deepcopy(mesg)

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

    def _parse_string(self, name):
        def _parser(mesg, data):
            if data[-1] != '\0':
                raise ProtocolError("Invalid string detected.")
            mesg[name] = data[:-1]
        return _parser

    def _parse_time(self, name):
        def _parser(mesg, data):
            if len(data) != 8:
                raise ProtocolError("Invalid time data length.")
            (val,) = struct.unpack("!Q", data)
            mesg[name] = float(val)
        return _parser

    def _parse_time_hires(self, name):
        def _parser(mesg, data):
            if len(data) != 8:
                raise ProtocolError("Invalid hires time data length.")
            (val,) = struct.unpack("!Q", data)
            mesg[name] = val * (2 ** -30)
        return _parser

    def _parse_values(self, name):
        types = {0: "!Q", 1: "<d", 2: "!q", 3: "!Q"}
        def _parser(mesg, data):
            (nvals,) = struct.unpack("!H", data[:2])
            data = data[2:]
            if len(data) != 9 * nvals:
                raise ProtocolError("Invalid value structure length.")
            vtypes = self.types.get(mesg["type"])
            if nvals != len(vtypes):
                raise ProtocolError("Values different than types.db info.")
            for i in range(nvals):
                (vtype,) = struct.unpack("B", data[i])
                if vtype != vtypes[i][1]:
                    raise ProtocolError("Type mismatch with types.db")
            data = data[nvals:]
            mesg[name] = {}
            for i in range(nvals):
                vdata, data = data[:8], data[8:]
                (val,) = struct.unpack(types[vtypes[i][1]], vdata)
                mesg[name][vtypes[i][0]] = val
        return _parser


class CollectDServer(object):
    def __init__(self, ip="0.0.0.0", port=25826):
        log.info("Creating collectd server.")
        self.parser = CollectDParser()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.bind((ip, port))
            log.info("Bound to %s:%s" % (ip, port))
        except OSError:
            raise BindError("Error opening collectd socket %s:%s." % (ip, port))

    def messages(self):
        while True:
            data, addr = self.sock.recvfrom(65535)
            try:
                for message in self.parser.parse(data):
                    yield message
            except ProtocolError, e:
                log.error("Protocol error: %s" % e)
