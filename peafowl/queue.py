# -*- coding: utf-8 -*-
import os, time, thread, logging
from Queue import Queue
from struct import pack, unpack

SOFT_LOG_MAX_SIZE = 16 * (1024**2) # 16 MB

TRX_CMD_PUSH = "\x00"
TRX_CMD_POP = "\x01"

TRX_PUSH = "\x00%s%s"
TRX_POP = "\x01"

class TransactionLogError(Exception):
    pass

class PersistentQueue(Queue):
    """
    PersistentQueue is a subclass of Python synchronized class. It adds a 
    transactional log to the in-memory Queue, which enables quickly rebuilding
    the Queue in the event of a sever outage.
    """
    def __init__(self, persistence_path, queue_name, debug = False):
        """
        Create a new PersistentQueue at ``persistence_path/queue_name``.
        If a queue log exists at that path, the Queue will be loaded from
        disk before being available for use.
        """
        self.persistence_path = persistence_path
        self.queue_name = queue_name
        self.transaction_lock = thread.allocate_lock()
        self.total_items = 0
        Queue.__init__(self, 0)
        self.initial_bytes = self._replay_transaction_log(debug)
    
    def put(self, value, log = True):
        """
        Pushes ``value`` to the queue. By default, ``put`` will write to the
        transactional log. Set ``log`` to ``False`` to override this behaviour.
        """
        if log:
            if not self.transaction_log:
                raise TransactionLogError("No transaction log")
            size = pack("I", len(value))
            self._transaction(TRX_PUSH % (size, value))
        self.total_items += 1
        Queue.put(self, value)
        
    def get(self, log = True):
        """
        Retrieves data from the queue.
        """
        if log and not self.transaction_log:
            raise TransactionLogError("No transaction log")
        value = Queue.get(self, log)
        if log:
            self._transaction("\001")
        return value
    
    def close(self):
        """
        Safely closes the transactional queue.
        """
        self.transaction_lock.acquire()
        not_trx = self.transaction_log
        self.transaction_log = None
        not_trx.close()
    
    def _rotate_log(self):
        self.transaction_log.close()
        os.rename(self._log_path(), "%s.%s" % (self._log_path(), time.time()))
        self._open_log()
    
    def _log_path(self):
        return os.path.join(self.persistence_path, self.queue_name)
    
    def _open_log(self):
        self.transaction_log = os.fdopen(os.open(self._log_path(), os.O_RDWR|os.O_CREAT), "rb+")
        self.log_size = os.path.getsize(self._log_path())
        
    def _replay_transaction_log(self, debug = False):
        self._open_log()
        bytes_read = 0
        logging.debug("Reading back transaction log for %s" % self.queue_name)
    
        while True:
            cmd = self.transaction_log.read(1)
            if not cmd:
                break;
            if cmd == TRX_CMD_PUSH:
                logging.debug(">")
                raw_size = self.transaction_log.read(4)
                size = unpack("I", raw_size)
                data = self.transaction_log.read(size[0])
                if not data:
                    continue
                self.put(data, False)
                bytes_read += len(data)
            elif cmd == TRX_CMD_POP:
                logging.debug("<")
                bytes_read -= len(self.get(False))
            else:
                logging.debug("Error reading transaction log: I don't understand '%s' (skipping)." % cmd)
        logging.debug("done.")
        return bytes_read
        
    def _transaction(self, data):
        if not self.transaction_log:
            raise TransactionLogError("No transaction log")
        
        try:
            self.transaction_lock.acquire()
            self.transaction_log.write(data)
            self.transaction_log.flush()
            self.log_size += len(data)
            if self.log_size > SOFT_LOG_MAX_SIZE and self.qsize() == 0:
                self._rotate_log()
        finally:
            self.transaction_lock.release()

