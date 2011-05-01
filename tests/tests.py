# -*- coding: utf-8 -*-
import unittest, random, time, hashlib, os
from peafowl.collection import QueueCollection, QueueCollectionError
try:
    from cmemcache import Client
except ImportError:
    from memcache import Client

class TestPeafowl(unittest.TestCase):
    def setUp(self):
        self.memcache = Client(['127.0.0.1:21122'])
        
    def tearDown(self):
        self.memcache.disconnect_all()

    def test_set_and_get_one_entry(self):
        v = random.randint(1, 32)
        self.assertEqual(None, self.memcache.get('test_set_and_get_one_entry'))
        self.memcache.set('test_set_and_get_one_entry', v)
        self.assertEqual(v, self.memcache.get('test_set_and_get_one_entry'))
    
    def test_set_with_expiry(self):
        v = random.randint(1, 32)
        self.assertEqual(None, self.memcache.get('test_set_with_expiry'))
        now = time.time()
        self.memcache.set('test_set_with_expiry', v + 2, now)
        self.memcache.set('test_set_with_expiry', v)
        time.sleep(now + 1 - time.time())
        self.assertEqual(v, self.memcache.get('test_set_with_expiry'))

    def test_long_value(self):
        string = ''.join([random.choice("abcdefghjkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789") for i in range(300)])
        self.memcache.set('test_long_value', string)
        string = self.memcache.get('test_long_value')
        self.assertEqual(len(string), 300)

    def test_stats(self):
        (key, stats) = self.memcache.get_stats()[0]
        self.assertEqual('127.0.0.1:21122 (1)', key)
        keys = ['bytes', 'pid', 'time', 'limit_maxbytes', 
                 'cmd_get', 'version', 'bytes_written', 'cmd_set', 
                 'get_misses', 'total_connections', 'curr_connections', 
                 'curr_items', 'uptime', 'get_hits', 'total_items', 
                 'rusage_system', 'rusage_user', 'bytes_read']
        for key in keys:
            self.assert_(stats.has_key(key), "key '%s' is not in stats" % key)
    
    def test_unknown_command_returns_valid_result(self):
        response = self.memcache.add('blah', 1)
        self.assertEqual(False, response)

    def test_that_disconnecting_and_reconnecting_works(self):
        v = random.randint(1, 32)
        self.memcache.set('test_that_disconnecting_and_reconnecting_works', v)
        self.memcache.disconnect_all()
        self.assertEqual(v, int(self.memcache.get('test_that_disconnecting_and_reconnecting_works')))

if __name__ == '__main__':
    unittest.main()

