
import logging
import optparse as op

import bucky.carbon as bcarbon
import bucky.collectd as bcollectd
import bucky.converter as bconverter


logging.basicConfig(format="[%(levelname)s] %(message)s", level=logging.DEBUG)


__usage__ = "%prog [OPTIONS]"


def options():
    return []


def main():
    parser = op.OptionParser(usage=__usage__, option_list=options())
    opts, args = parser.parse_args()

    cli = bcarbon.CarbonClient()
    srv = bcollectd.CollectDServer()
    cnv = bconverter.BuckyConverter()

    for mesg in srv.messages():
        for stat, value, time in cnv.convert(mesg):
            cli.send(stat, value, time)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        raise # debug
        print e
