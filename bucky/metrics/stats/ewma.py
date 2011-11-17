import math

class EWMA(object):
    """\
    Exponentially-weighted moving avergage. Based on the
    implementation in Coda Hale's metrics library:
    
       https://github.com/codahale/metrics/blob/development/metrics-core/src/main/java/com/yammer/metrics/stats/EWMA.java
    """

    M1_ALPHA = 1 - math.exp(-5.0 / 60.0)
    M5_ALPHA = 1 - math.exp(-5.0 / 60.0 / 5.0)
    M15_ALPHA = 1 - math.exp(-5.0 / 60.0 / 15.0)

    @staticmethod
    def oneMinuteEWMA():
        return EWMA(EWMA.M1_ALPHA, 5.0)
    
    @staticmethod
    def fiveMinuteEWMA():
        return EWMA(EWMA.M5_ALPHA, 5.0)
    
    @staticmethod
    def fifteenMinuteEWMA():
        return EWMA(EWMA.M15_ALPHA, 5.0)

    def __init__(self, alpha, interval):
        self.alpha = alpha
        self.interval = interval
        self.curr_rate = None
        self.uncounted = 0L
    
    def update(self, val):
        self.uncounted += val
    
    def rate(self):
        return self.curr_rate
    
    def tick(self):
        count = self.uncounted
        self.uncounted = 0L
        instant_rate = count / self.interval
        if self.initialized:
            self.curr_rate += (self.alpha * (instant_rate - self.curr_rate))
        else:
            self.curr_rate = instant_rate

