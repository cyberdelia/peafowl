# -*- coding: utf-8 -*-
import os, thread
from queue import PersistentQueue

class QueueCollectionError(Exception):
    pass

class QueueCollection(object):
    def __init__(self, path):
        if not os.path.isdir(path) and not os.access(path, os.W_OK):
            raise QueueCollectionError("Queue path '%s' is inacessible" % path) 
        self.shutdown_lock = thread.allocate_lock()
        self.path = path
        self.queues = {}
        self.queue_locks = {}
        self.stats = {'current_bytes':0, 'total_items':0, 'get_misses':0, 'get_hits':0}
    
    def put(self, key, data):
        """
        Puts ``data`` onto the queue named ``key``.
        """
        queue = self.get_queues(key)
        if not queue:
            return None
        self.stats['current_bytes'] += len(data)
        self.stats['total_items'] += 1
        queue.put(data)
        return True
    
    def take(self, key):
        """
        Retrieves data from the queue named ``key``.
        """
        queue = self.get_queues(key)
        if not queue or not queue.qsize():
            self.stats['get_misses'] += 1
            return None
        else:
            self.stats['get_hits'] += 1
        result = queue.get()
        self.stats['current_bytes'] -= len(result)
        return result
    
    def get_queues(self, key = None):
        """
        Returns all active queues.
        """
        if self.shutdown_lock.locked():
            return None
            
        if not key:
            return self.queues
        
        if self.queues.has_key(key):
            return self.queues[key]
        
        if not self.queue_locks.has_key(key):
            self.queue_locks[key] = thread.allocate_lock()
        
        if self.queue_locks[key].locked():
            return None
        else:
            try:
                self.queue_locks[key].acquire()
                if not self.queues.has_key(key):
                    self.queues[key] = PersistentQueue(self.path, key)
                    self.stats['current_bytes'] += self.queues[key].initial_bytes
            finally:
                self.queue_locks[key].release()
        return self.queues[key]
        
    def get_stats(self, name = None):
        """
        Returns statistic ``stat_name`` for the ``QueueCollection``.
        Valid statistics are:
            ``get_misses``    Total number of get requests with empty responses
            ``get_hits``      Total number of get requests that returned data
            ``current_bytes`` Current size in bytes of items in the queues
            ``current_size``  Current number of items across all queues
            ``total_items``   Total number of items stored in queues.
        """
        if not name:
            return self.stats
        elif name == 'current_size':
            return self._current_size()
        else:
            return self.stats[name]
    
    def close(self):
        """
        Safely close all queues.
        """
        self.shutdown_lock.acquire()
        for name, queue in self.queues:
            queue.close()
            del self.queues[name]
    
    def _current_size(self):
        size = 0
        for name in self.queues:
            size += self.queues[name].qsize()
        return size
        
