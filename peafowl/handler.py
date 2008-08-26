# -*- coding: utf-8 -*-
import re, time, os, logging, socket, errno, threading
from resource import getrusage, RUSAGE_SELF
from struct import pack, unpack

DATA_PACK_FMT = "!II%sp"

# ERROR responses
ERR_UNKNOWN_COMMAND = "CLIENT_ERROR bad command line format\r\n"

# GET Responses
GET_COMMAND = r'^get (.{1,250})\r\n$'
GET_RESPONSE = "VALUE %s %s %s\r\n%s\r\nEND\r\n"
GET_RESPONSE_EMPTY = "END\r\n"

# SET Responses
SET_COMMAND = r'^set (.{1,250}) ([0-9]+) ([0-9]+) ([0-9]+)\r\n$'
SET_RESPONSE_SUCCESS  = "STORED\r\n"
SET_RESPONSE_FAILURE  = "NOT STORED\r\n"
SET_CLIENT_DATA_ERROR = "CLIENT_ERROR bad data chunk\r\nERROR\r\n"

# STAT Response
STATS_COMMAND = r'stats\r\n$'
STATS_RESPONSE = """STAT pid %d\r
STAT uptime %d\r
STAT time %d\r
STAT version %s\r
STAT rusage_user %0.6f\r
STAT rusage_system %0.6f\r
STAT curr_items %d\r
STAT total_items %d\r
STAT bytes %d\r
STAT curr_connections %d\r
STAT total_connections %d\r
STAT cmd_get %d\r
STAT cmd_set %d\r
STAT get_hits %d\r
STAT get_misses %d\r
STAT bytes_read %d\r
STAT bytes_written %d\r
STAT limit_maxbytes %d\r
%s\nEND\r\n"""
QUEUE_STATS_RESPONSE = """STAT queue_%s_items %d\r
STAT queue_%s_total_items %d\r
STAT queue_%s_logsize %d\r
STAT queue_%s_expired_items %d\r"""

class Handler(threading.Thread):
    """
    This is an internal class used by Peafowl Server to handle the
    MemCache protocol and act as an interface between the Server and the
    QueueCollection.
    """
    def __init__(self, socket, queue_collection, stats):
        threading.Thread.__init__(self)
        self.expiry_stats = {}
        self.stats = stats
        self.queue_collection = queue_collection
        self.socket = socket
        self.file = self.socket.makefile("rb")

    def run(self):
        """
        Process incoming commands from the attached client.
        """
        self.stats['total_connections'] += 1
        while True:
            try:
                command = self.file.readline()
                if not command:
                    break
                logging.debug("Receiving command : %s" % repr(command))
                self.stats['bytes_read'] += len(command)
                self._process(command)
            except socket.timeout, (value, message):
                logging.info("Shutdown due to timeout: %s" % message)
                self.socket.close()
            except socket.error, (value, message):
                if value == errno.EMFILE:
                    # we should do something less stupid
                    logging.warning("Too many open files or sockets")
                else:
                    self.socket.close()
    
    def _process(self, command):
        m = re.match(SET_COMMAND, command)
        if m:
            logging.debug("Received a SET command")
            self.stats['set_requests'] += 1
            self.set(m.group(1), m.group(2), m.group(3), m.group(4))
            return
        m = re.match(GET_COMMAND, command)
        if m:
            logging.debug("Received a GET command")
            self.stats['get_requests'] += 1
            self.get(m.group(1))
            return
        m = re.match(STATS_COMMAND, command)
        if m:
            logging.debug("Received a STATS command")
            self.get_stats()
            return
        logging.debug("Received unknow command")
        self._respond(ERR_UNKNOWN_COMMAND)
    
    def _respond(self, message, *args):
        response = message % args
        self.stats['bytes_written'] += len(response)
        logging.debug("Sending response : %s" % repr(response))
        self.socket.send(response)
    
    def set(self, key, flags, expiry, length):
        length = int(length)
        data = self.file.read(length)
        data_end = self.file.read(2)
        self.stats['bytes_read'] += (length + 2)
        if data_end == '\r\n' and len(data) == length:
            internal_data = pack(DATA_PACK_FMT % (length + 1), int(flags), int(expiry), data)
            if self.queue_collection.put(key, internal_data):
                logging.debug("SET command is a success")
                self._respond(SET_RESPONSE_SUCCESS)
            else:
                logging.warning("SET command failed")
                self._respond(SET_RESPONSE_FAILURE)
        else:
            logging.error("SET command failed hard")
            self._respond(SET_CLIENT_DATA_ERROR)
    
    def get(self, key):
        now = time.time()
        data = None
        response = self.queue_collection.take(key)
        while response:
            flags, expiry, data = unpack(DATA_PACK_FMT % (len(response) - 8), response)
            if expiry == 0 or expiry >= now:
                break
            if self.expiry_stats.has_key(key):
                self.expiry_stats[key] += 1
            else:
                self.expiry_stats[key] = 1
            flags, expiry, data = None, None, None
            response = self.queue_collection.take(key)
        if data:
            logging.debug("GET command respond with value")
            self._respond(GET_RESPONSE, key, flags, len(data), data)
        else:
            logging.debug("GET command response was empty")
            self._respond(GET_RESPONSE_EMPTY)
    
    def get_stats(self):
        self._respond(STATS_RESPONSE,
            os.getpid(), # pid
            time.time() - self.stats['start_time'], # uptime
            time.time(), # time
            '0.4', # peafowl version
            getrusage(RUSAGE_SELF)[0],
            getrusage(RUSAGE_SELF)[1], 
            self.queue_collection.get_stats('current_size'),
            self.queue_collection.get_stats('total_items'),
            self.queue_collection.get_stats('current_bytes'),
            self.stats['connections'],
            self.stats['total_connections'],
            self.stats['get_requests'],
            self.stats['set_requests'],
            self.queue_collection.stats['get_hits'],
            self.queue_collection.stats['get_misses'],
            self.stats['bytes_read'],
            self.stats['bytes_written'],
            0,
            self.queue_stats()    
        )
        
    def queue_stats(self):
        response = ''
        for name in self.queue_collection.get_queues():
            queue = self.queue_collection.get_queues(name)
            if self.expiry_stats.has_key(name):
                expiry_stats = self.expiry_stats[name]
            else:
                expiry_stats = 0
            response += QUEUE_STATS_RESPONSE % (name, queue.qsize(), name, queue.total_items, name, queue.log_size, name, expiry_stats)
        return response

    
