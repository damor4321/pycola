import os
import sys
import threading
import urllib
import time
import logging
import logging.handlers
import hashlib
import pprint
import getopt



# Global vars.
# configuration:
items = 10
delay = 5
trys = 5
retry = 5
limit = 1000
path_queue = ""
path_log = ""
logfile = ""

#internals

valid_status = [302,200,304]

data = []
retryStore = {}
activeStore = {}

pp = pprint.PrettyPrinter(indent=4)
####################



class Queue:

    def __init__(self):
        logging.debug("Starting queue PID:"+ str(os.getpid()) +" over "+ path_queue)
        pass

    def loop (self):
        
        driver = Driver()
        
        while True:

            executed = driver.step()
            if not executed: driver.wait()
            time.sleep(0.001)
    
        #out of the while: the children die alone, there is no need for a terminate them.
        logging.debug("Ended.")
        print "END"




class Driver:


    def __init__ (self):
        self.threads = 0
        pass



    def step (self):

        self.last_step_time = time.time()

        item_list = self.get_items()
        launched = 0

        while item_list:

            active = threading.activeCount() - 1
            free_slots = items - active
            items_num = len(item_list)
            launched = 0

            to_launch = free_slots if free_slots < items_num  else items_num            
            
            logging.debug("[STEP] items:" +str (items_num) + " max:" + str(items)  + " free:" + str(free_slots) + " to_launch:" + str(to_launch))
            
            for x in xrange(to_launch):
                item = item_list.pop(0)[1]
                
                if self.is_processable(item):
                    thr = ItemThr (item, self.tester)
                    thr.start()
                    launched += 1 
                    self.threads += 1
                
                #time.sleep(0.01) 
            
            logging.debug("[STEP] launched:" + str(launched))

            if launched < items: time.sleep(0.2)

        self.purge_active()

        return launched
        
        
        
    def purge_active(self):
        lock_time = time.time() - 60 # items with more than 1 minute of time execution. Their execution is ignored.
        lock_count = 0
        
        for k, v in activeStore.copy().iteritems():
            if v < lock_time:
                del activeStore[k]
                lock_count += 1
                
        if lock_count > 0:
            logging.debug("[STEP] active purged:" + str(lock_count))



    def wait(self):

        bw_steps = time.time() - self.last_step_time
        if bw_steps < delay:
            wait = delay - bw_steps
            time.sleep(wait)
        
    
    
    def  get_items (self):
        global data

        ut = Utils()
        data = []
        items = []        
        
        range = os.listdir(path_queue)
        range.sort()

        for interval in range:
            if ut.rGetItems(os.path.join(path_queue, interval)): break
            
            if (not data) and (int(interval) < time.time() - 60*5):
                self.purgeOld(os.path.join(path_queue, interval))

        for path in data:
            try:
                mtime = str (os.path.getctime(path)) + "-" + hashlib.sha1(path).hexdigest()
                if not activeStore.has_key(path):
                    items.append( (mtime, path) )
            except: pass

        ut.sortby(items,0)


        return items        



    def is_processable (self, path):
        
        if activeStore.has_key(path):
            logging.debug("[CHECK] " + path +" active")
            return False
       
        if retryStore.has_key(path):
            try:
                if retryStore[path].last_time > (time.time() - retry):
                    return False
            except: return False
            
            logging.debug( "[CHECK]" + path +" retry OK" )
            
        return True
        
        
    
    def purgeOld (self, path):

        ut = Utils()
        try:
            ut.rmAll(path)
            logging.debug("Purge interval " + path + " (now "+ str(time.time()) +")" )
        except os.error:
            logging.debug("Purge interval " + path + ": FAIL" )
            
            
    def tester(self, item):
        self.threads -= 1
        logging.debug("tester "+ str(self.threads) +" "+ item +" " + str(time.time()) )


    


class ItemThr ( threading.Thread ):

    def __init__(self, item, callback):
        threading.Thread.__init__(self)

        activeStore[item] = time.time()
        self.time_start = time.time()
        
        self.item = item
        self.url = ""     
        self.status = 0
        self.hash = self.item.replace(path_queue, '')
        self.callback = callback
    

    def run ( self ):
    
        if self.process():
            self.trace('OK')
            self.delete()
            
        else:
            self.trace('FAILURE')
            self.mark_retry()
        
        self.alive()

        del activeStore[self.item]
        self.callback(self.item)


    def process (self):
        
        try:
            f = open(self.item , 'r')
            self.url = (f.readline()).strip()
            f.close()
        except: return False

        try:
            r = urllib.urlopen(self.url) 
            self.status = r.getcode()
        except: pass
        
        return (self.status in valid_status)


    def mark_retry (self):

        try:
            if os.path.isfile(self.item): 
                retryStore[self.item].inc()
                
                if retryStore[self.item].count >= trys:
                    logging.debug(self.hash +" last try")
                    self.delete()
                    return
                    
                self.touch()
                logging.debug(self.hash +" inc retry:"+ str(retryStore[self.item].count)) 
                
        except KeyError:
            retryStore[self.item] = Retry()
            logging.debug(self.hash +" new retry") 
              
        except: return
          

    def trace (self, msg):
        log = "%s status[%s] url[%s] msg[%s]" % (self.hash, str(self.status), self.url, msg)
        logging.debug(log)
        

    def touch(self):
        open(self.item, 'a').close()
        os.utime(self.item, None)
        
        
    def alive(self):
        wait_to_die = 1 - (time.time() - self.time_start)
        
        # If we have to wait more than 0.1, we will not do it
        if wait_to_die > 0.001 and wait_to_die < 1:
            #logging.debug (self.hash +'wait '+ str(self.wait_to_die) +' to die')
            time.sleep(wait_to_die)
            
            
    def delete(self):
        try:
            os.remove(self.item)
            del retryStore[self.item]
        except: pass
        logging.debug(self.item +" deleted") 




class Retry:
    
    def __init__ (self): 
        self.count = 1
        self.last_time = time.time()
        self.creation_time = time.time()
    
    def inc (self):
        self.count += 1
        self.last_time = time.time()
    
    def __toString(self): 
        return self.count




###################################
class Utils:

    def __init__ (self):
        pass

    
    def rGetItems (self, path_queue):
        global data

        dirAndFileList = os.listdir(path_queue)
        dirAndFileList.sort()
  
        for i in dirAndFileList:
            complete_path = os.path.join(path_queue, i)

            if os.path.isdir(complete_path):
                exit = self.rGetItems(complete_path)
                if exit: return exit

            elif os.path.isfile(complete_path):
                data.append(complete_path)
                if len(data) >= limit: return True

        return False


    def rmAll(self, path):                           
        
        list = os.listdir(path)
        for name in list:                      
            child_path = os.path.join(path, name)
            if not os.path.isdir(child_path):
                os.remove(child_path)
            else:                                   
               self.rmAll(child_path)
        
        os.rmdir(path)                           



    def  getOptions(self):
        global items, delay, trys, retry, path_queue, limit, path_log, valid_status, logfile
    
        try:
            opts, args = getopt.getopt(sys.argv[1:], 'h',['help', 'path-queue=', 'path-log=', 'items=', 'delay=', 'trys=', 'retry=', 'block-size=', 'valid-status='])
        except getopt.GetoptError, err:
            print str(err)
            self.usage()
            sys.exit(2)

        for o, a in opts:
            if o in ("-h", "--help"):
                self.usage()
                sys.exit(1)
            elif o in ("--path-queue"): path_queue = a
            elif o in ("--path-log"): 
                path_log = a
                logfile = path_log + os.sep + "queues_py.log"
            elif o in ("--items"): items = int(a)
            elif o in ("--delay"): delay = int(a)
            elif o in ("--trys"): trys = int(a)
            elif o in ("--retry"): retry = int(a)
            elif o in ("--block-size"): limit = int(a)
            elif o in ("--valid-status") and self.validate(a): pass
            else: assert False, "unhandled option"
    
        if not path_queue or not path_log:
            self.usage()
            sys.exit(2)


    def usage (self):
        global items, delay, trys, retry, path_queue, limit

        print " python " + sys.argv[0] + " <param=value>"
        print
        print " Params:"
        print
        print " --path-queue    : [%s] path of the item queue (mandatory)" % path_queue
        print " --path-log      : [%s] path of the item queue (mandatory)" % path_log
        print " --items         : [%d] number of processed items per second" % items
        print " --delay         : [%d] time to extract items from the queue" % delay
        print " --trys          : [%d] number of retries in processing an item" % trys
        print " --retry         : [%d] the time between retries" % retry
        print " --block-size    : [%d] the number of items reloaded between delays" % limit
        print " --valid-status  : [%s] the number of items reloaded between delays" % ",".join(map(str,valid_status))
        print
            
    def validate (self, status):
        global valid_status

        temp = map(int, status.split(','))
        if temp: 
            valid_status = temp
            return True
        return False




    def sortby(self, list, n):
        list[:] = [(x[n], x) for x in list]
        list.sort()
        list[:] = [val for (key, val) in list]
        return
        
        
    def init_logging (self):
        logging.getLogger().setLevel(logging.DEBUG) # override default of WARNING

        lfile = logging.handlers.TimedRotatingFileHandler(logfile , 'midnight', 1, backupCount=14)
        lfile.setLevel(logging.DEBUG)
        lfile.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(threadName)s %(message)s'))
        logging.getLogger().addHandler(lfile)






def main():

    ut = Utils()
    ut.getOptions()
    ut.init_logging()
    
    queue = Queue()
    queue.loop()
    

try :
    main()
except KeyboardInterrupt:
    sys.exit()




