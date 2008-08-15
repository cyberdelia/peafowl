# -*- coding: utf-8 -*-
from optparse import OptionParser
import server, os, sys, errno, signal, logging, string, traceback, time

class Runner(object):
    def __init__(self):
        (options, args) = self.parse_options()
        self.process = ProcessHelper(options.log_file, options.pid_file, options.user, options.group)
        self.server = None
        pid = self.process.is_running()
        if pid:
            sys.stderr.write("There is already a Peafowl process running (pid %s), exiting.\n" % pid)
            sys.exit(1)
        elif not pid:
            sys.stderr.write("Cleaning up stale pidfile at %s.\n" % options.pid_file)
        self.start(options)
        
    @staticmethod
    def run():
        Runner()
        
    def parse_options(self):
        parser = OptionParser()
        parser.add_option("-q", "--queue", action="store", type="string", dest="path", help="path to store Peafowl queue logs", default=server.DEFAULT_PATH)
        parser.add_option("-H", "--host", action="store", type="string", dest="host", help="interface on which to listen", default=server.DEFAULT_HOST)
        parser.add_option("-p", "--port", action="store", type="int", dest="port", help="TCP port on which to listen", default=server.DEFAULT_PORT)
        parser.add_option("-d", action="store_true", dest="daemonize", help="run as a daemon", default=False)
        parser.add_option("-P", "--pid", action="store", type="string", dest="pid_file", help="save pid in PID_FILE when using -d option", default=server.DEFAULT_PID)
        parser.add_option("-u", "--user", action="store", type="int", dest="user", help="user to run as")
        parser.add_option("-g", "--group", action="store", type="int", dest="group", help="group to run as")
        parser.add_option("-l", "--log", action="store", type="string", dest="log_file", help="path to print debugging information")
        parser.add_option("-v", action="count", dest="verbosity", help="increase logging verbosity", default=0)
        parser.set_defaults(verbose=True)
        return parser.parse_args()
    
    def start(self, options):
        if options.user:
            os.seteuid(options.user)
        if options.group:
            os.setegid(options.group)
            
        if options.daemonize:
            self.process.daemonize()
        
        self.trap_signals()
        self.process.write_pid_file()
        self.server = server.Server.start(host=options.host, port=options.port, path=options.path, debug=options.verbosity * 10)
        self.process.remove_pid_file()
    
    def shutdown(self, signal, frame):
        try:
            print "Shutting down."
            logging.info("Shutting down.")
            if self.server:
                self.server.stop()
        except Exception, e:
            trace = string.join(traceback.format_exception(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]))
            sys.stderr.write("There was an error shutting down: %s\n%s" % (e, trace))
            sys.exit(70)
    
    def trap_signals(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        
class ProcessHelper(object):
    def __init__(self, log_file = None, pid_file = None, user = None, group = None):
        self.log_file = log_file
        self.pid_file = pid_file
        self.user = user
        self.group= group
    
    def safe_fork(self):
        try:
            return os.fork()
        except OSError, e:
            if e.errno == errno.EWOULDBLOCK:
                time.sleep(5)
                self.safe_fork()
    
    def daemonize(self):
        pid = self.safe_fork()
        if pid > 0:
            sys.exit(0)
        os.chdir("/")
        sess_id = os.setsid()
        os.umask(0)
        pid = self.safe_fork()
        if pid > 0:
            sys.exit(0)
        return sess_id
    
    def is_running(self):
        if not self.pid_file:
            return False 
        try:
            pid_file = open(self.pid_file, 'r')
            pid = int(pid_file.read())
            pid_file.close()
            if pid == 0:
                return False
        except IOError, e:
            return False
        
        try:
            os.kill(pid, 0)
            return pid
        except OSError, e:
            if e.errno == errno.ESRCH:
                return None
            elif e.errno == errno.EPERM:
                return pid
    
    def write_pid_file(self):
        if not self.pid_file:
            return
        pid_file = open(self.pid_file, "w")
        pid_file.write(str(os.getpid()))
        os.chmod(self.pid_file, 0644)
        
    def remove_pid_file(self):
        if not self.pid_file:
            return
        os.remove(self.pid_file)
    
        
