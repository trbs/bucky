
import logging
import optparse as op
import Queue

import bucky.carbon as carbon
import bucky.collectd as collectd
import bucky.statsd as statsd


logging.basicConfig(format="[%(levelname)s] %(message)s", level=logging.DEBUG)


__usage__ = "%prog [OPTIONS]"


def options():
    return []


def main():
    parser = op.OptionParser(usage=__usage__, option_list=options())
    opts, args = parser.parse_args()

    sampleq = Queue.Queue()

    cdsrv = collectd.CollectDServer(sampleq)
    cdsrv.start()

    stsrv = statsd.StatsDServer(sampleq)
    stsrv.start()

    cli = carbon.CarbonClient()

    while True:
        try:
            stat, value, time = sampleq.get(True, 1)
            cli.send(stat, value, time)
        except Queue.Empty:
            pass
        if not cdsrv.is_alive():
            log.error("collectd server died")
            break
        if not stsrv.is_alive():
            log.error("statsd server died")
            break


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        raise # debug
        print e
