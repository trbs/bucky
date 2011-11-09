
import socket
import time


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
        except OSError:
            raise ConnectError("Failed to connect to %s:%s" % (ip, port))
        self.last_flush = time.time()

    def send(self, stat, value, time):
        mesg = "%s %s %s\n" % (stat, value, time)
        self.sock.sendall(mesg)
        if time.time() - self.last_flush > 10:
            self.sock.flush()
            self.last_flush = time.time()

