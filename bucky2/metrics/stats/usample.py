
import random

class UniformSample(object):
    """\
    A random sample of a stream of long's based on the
    implementation in Coda Hale's Metrics library:
    
        https://github.com/codahale/metrics/blob/development/metrics-core/src/main/java/com/yammer/metrics/stats/UniformSample.java    
    """

    def __init__(self, size):
        self.count = 0
        self.values = [0] * size

    def clear(self):
        self.count = 0
        for i in range(len(self.values)):
            self.values[i] = 0

    def size(self):
        if self.count > len(self.values):
            return len(self.values)
        return self.count

    def update(self, val):
        self.count += 1
        if self.count <= len(self.values):
            self.values[self.count-1] = val
        else:
            r = random.random(0, self.count-1)
            if r < len(self.values):
                self.values[r] = val
    
    def get_values(self):
        return self.values[:]
