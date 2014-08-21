import time

import t
import bucky.helpers


def test_file_monitor():
    path = t.temp_file('asd')
    monitor = bucky.helpers.FileMonitor(path)
    t.eq(monitor.modified(), False)
    with open(path, 'w') as f:
        f.write('bbbb')
    time.sleep(1)
    t.eq(monitor.modified(), True)
    t.eq(monitor.modified(), False)
