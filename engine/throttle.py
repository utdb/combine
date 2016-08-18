import time


class Throttle:
    """
    This throttling class is not thread-safe.
    """
    def __init__(self, min_delay):
        self.min_delay = min_delay
        self.last = 0

    def wait(self):
        delta = time.time() - self.last
        # print("WAIT: "+str(delta - self.min_delay))
        if delta < self.min_delay:
            time.sleep(self.min_delay - delta)
        self.last = time.time()

#

DEFAULT_DELAY = 1.0

global_throttle = {}


def add_throttle(domainname, min_delay):
    global_throttle[domainname] = Throttle(min_delay)


def get_throttle(domainname):
    if global_throttle.get(domainname) is None:
        add_throttle(domainname, DEFAULT_DELAY)
    return global_throttle[domainname]


def wait_for(domainname):
    get_throttle(domainname).wait()
