
import bucky.cfg as cfg

__host_trim__ = None

def _get_host_trim():
    global __host_trim__
    if __host_trim__ is not None:
        return __host_trim__
    host_trim = cfg.name_host_trim
    __host_tim__ = []
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


def statname(host, nameparts):
    parts = []
    if self.prefix:
        parts.append(self.prefix)
    parts.extend(hostname(host))
    parts.extend(nameparts)
    if self.postfix:
        parts.append(self.postfix)
    if self.replace is not None:
        parts = [p.replace(".", cfg.name_replace_char) for p in parts]
    if self.strip_duplicates:
        parts = strip_duplicates(parts)
    return ".".join(parts)

