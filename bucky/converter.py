

class BuckyConverter(object):
    def __init__(self, prefix=None, postfix=None, replace="_", strip=None):
        self.prefix = prefix
        self.postfix = postfix
        self.replace = replace
        self.strip = []
        if strip is not None:
            for s in strip:
                s = list(reversed(p.strip() for p in s.split(".")))
                self.strip.append(s)

    def convert(self, mesg):
        for val in mesg["values"]:
            yield self.stat(mesg, val), self.value(mesg, val), self.time(mesg)

    def stat(self, mesg, val):
        parts = []
        if self.prefix:
            parts.append(self.prefix)
        hostparts = mesg.get("host", "").split(".")
        hostparts = list(reversed([hp.strip() for hp in hostparts]))
        for s in self.strip:
            same = True
            for i, p in enumerate(s):
                if p != hostparts[i]:
                    same = False
                    break
            if same:
                hostparts = hostparts[len(s):]
        parts.extend(hostparts)
        def maybe_append(val):
            p = mesg.get(val, "")
            if p.strip():
                parts.append(p)
        parts.append(mesg["plugin"].strip())
        if mesg.get("plugin_instance"):
            parts.append(mesg["plugin_instance"].strip())
        mtype = mesg.get("type", "").strip()
        if mtype and mtype != "value":
            parts.append(mtype)
        mtypei = mesg.get("type_instance", "").strip()
        if mtypei:
            parts.append(mtypei)
        mvals = mesg.get("values", [])
        if len(mvals) > 1 or val != "value":
            parts.append(val.strip())
        parts = [p.replace(".", self.replace) for p in parts]
        i = 1
        while i < len(parts):
            if parts[i] == parts[i-1]:
                parts.pop(i)
            else:
                i += 1
        return ".".join(parts)

    def value(self, mesg, val):
        return mesg["values"][val]

    def time(self, mesg):
        return int(mesg["time"])

