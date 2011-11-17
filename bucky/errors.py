
class BuckyError(Exception):
    def __init__(self, mesg):
        self.mesg = mesg
    def __str__(self):
        return self.mesg


class ConnectError(BuckyError):
    pass


class ConfigError(BuckyError):
    pass


class ProtocolError(BuckyError):
    pass
