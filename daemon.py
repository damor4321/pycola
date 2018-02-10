import os
import atexit
import time
        

class Daemon:

    def __init__(self, pidfile):
        self.pidfile = pidfile
        
        
    def is_unique_execution(self):
        
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
            
            procfile = file("/proc/%d/status" % pid, 'r')
            procfile.close()
            
        except IOError:
            return True
        except TypeError:
            return True
        
        return False
    
    
    def deletepid(self):
        os.remove(self.pidfile)
        
        
    def recordpid(self):
        atexit.register(self.deletepid)
        pid = str(os.getpid())
        try:
            os.mkdir(os.path.dirname(self.pidfile))
        except:
            pass
        file(self.pidfile, 'w+').write("%s\n" % pid)
        

    def touch(self):
        open(self.pidfile, 'a').close()
        os.utime(self.pidfile, None)
        
        
    def touchinterval(self):
        while True:
            self.touch()
            time.sleep(30)
