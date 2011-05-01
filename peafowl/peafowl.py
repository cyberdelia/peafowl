# -*- coding: utf-8 -*-
import time
try:
    import cmemcache as memcache
except ImportError:
    import memcache

WAIT_TIME = 0.25

class PeafowlError(Exception):
    pass
    
class Peafowl(memcache.Client):
    def get(self, *args, **kwargs):
        while True:
            response = super(Peafowl, self).get(*args, **kwargs)
            if response:
                return response
            time.sleep(WAIT_TIME)
    
    def set(self, *args, **kwargs):
        retries = 0
        while retries < 3:
            return_value = super(Peafowl, self).set(*args, **kwargs)
            if not return_value:
                retries += 1
                time.sleep(WAIT_TIME)
            else:
                break;
        else:
            raise PeafowlError("Can't set value")

    def __len__(self, *args, **kwargs):
        statistics = self.get_stats()
        return int(statistics[0][1]['total_items'])

