
import logging
import socket


log = logging.getLogger(__name__)


class CarbonException(Exception):
    def __init__(self, mesg):
        self.mesg = mesg
    def __str__(self):
        return self.mesg


class ConnectError(CarbonException):
    pass


class CarbonClient(object):
    def __init__(self, ip="127.0.0.1", port=2003):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((ip, port))
            log.info("Connect to Carbon at %s:%s" % (ip, port))
        except OSError:
            raise ConnectError("Failed to connect to %s:%s" % (ip, port))

    def send(self, stat, value, mtime):
        mesg = "%s %s %s\n" % (stat, value, mtime)
        self.sock.sendall(mesg)

