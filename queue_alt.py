import os
import sys
import threading
import urllib
import time
import logging
import hashlib
import pprint
import getopt



# Global vars.
# configuration:
items = 10 
delay = 1
trys = 5
retry = 5
limit = 20
wait = 0
path_queue = ""

#internals
path_logs =     "/tmp/damor/log"
logfile = path_logs + os.sep + "queues_py.log"
valid_status = [302,200,304]

data = []
retryStore = {}

pp = pprint.PrettyPrinter(indent=4)
####################


#############################

class Queue:

    def __init__(self):        
        self.items = items
        logging.basicConfig(filename=logfile,level=logging.DEBUG,format='%(asctime)s %(levelname)s %(threadName)s %(message)s')

    def loop (self):
        
        driver = Driver()


        while True:

            active = threading.activeCount() - 1
            to_launch = self.items - active
            
            Items = driver.getItems()
       
            logging.debug("# of items in the queue: " + str (len(Items)))         
            logging.debug("(# of items in the retryStore: " + str (len(retryStore)) + ")")


            logging.debug( "Totals: " + str (items) + "| Active: " + str (active) )
        
            thr_cont = 0
            for x in xrange ( to_launch ):
                if Items:
                    item = Items.pop(0)[1]
                    thr = ItemThr (item)
                    thr.start()
                    thr_cont += 1
                
                time.sleep (wait)

            logging.debug("Se han lanzado: " + str(thr_cont))
            time.sleep (delay)

        #out of while: the children die alone, there is no need for a terminate.
        logging.debug("Ended.")
        print "END"




class Driver:

    def __init__ (self): 
        global retry
        self.last_time = time.time()



    def  getItems (self):
        global data

        self.last_time = time.time()

        ut = Utils()
        data = []
        items = []        
        
        range = os.listdir(path_queue)
        range.sort()
        for interval in range: 
            if ut.rGetItems(os.path.join(path_queue, interval)): break

       
        # you do not need data.sort(), which also reach the same result because data (because ut.rGetItems) is already sorting.
        for path in data:
            mtime = str (os.path.getctime(path)) + "-" + hashlib.sha1(path).hexdigest()
            if self.manageRetry(path): items.append( (mtime, path) )

        ut.sortby(items,0)
        return items        



    def manageRetry (self, path):
        global retryStore
       
        if retryStore.has_key (path):
            logging.debug("Driver: THIS WAS ALREADY LAUNCHED BEFORE: count: " + str(retryStore[path].count) + " & last_time:" + str(retryStore[path].last_time)) 
            
            if retryStore[path].count >= trys:
                try:
                    del retryStore[path]
                    os.remove (path)
                    logging.debug("Driver: The retry limit for the item " + path + " was reached") 
                except: pass
                return False
            
            if retryStore[path].last_time <= (self.last_time - retry):
                retryStore[path].inc()
                logging.debug("Driver: The number of retries is increased for item " + path + ", retries: " + str(retryStore[path].count)) 
                return False

            #no se lanzo porque no habia slots disponibles, se extrae para lanzarlo
            return True

        logging.debug("Driver: IT IS NEW ITEM: " + path + ", (it's included)") 
        retryStore[path] = Retry()
        return True
        



class ItemThr ( threading.Thread ):

    def __init__(self, item): 

        threading.Thread.__init__(self)  
        self.item = item
        self.url = ""        
        self.status = 0    
    

    def run ( self ):
        res = self.process()
        if res:
            try: 
                os.remove (self.item)
                del retryStore[self.item]
            except: pass
            self.trace ('terminated with SUCCESS', self.item, self.url, str(self.status))
            sys.exit()
        
        self.mark_retry()
        self.trace ('terminated with FAILURE', self.item, self.url, str(self.status))



    def process (self):

        try:
            f = open(self.item , 'r')
            self.url = (f.readline()).strip()
            f.close()
        except: return 0

        try:
            r = urllib.urlopen(self.url) 
            self.status = r.getcode()
            #for testing
            #time.sleep(random.randint(1, 10))
        except: pass
        
        for status in valid_status: 
            if self.status == status: return self.status
        
        return 0


    def mark_retry (self):
        if os.path.isfile(self.item): 
            try:
                self.touch(self.item)
            except os.error: return


    def trace (self, msg, file, url, res):
        logging.debug(': ' + msg)
        logging.debug(': ' + 'file: ' + file)
        logging.debug(': ' + 'url: ' + url)
        logging.debug(': ' + 'response status: ' + res)


    def touch(self, file):
        open(file, 'a').close()
        os.utime(file, None)



class Retry:
    
    def __init__ (self): 
        self.count = 1
        self.last_time = time.time()
    
    def inc (self):
        self.count = self.count + 1
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




    def  getOptions(self):
        global items, delay, trys, retry, wait, path_queue, limit
    
        try:
            opts, args = getopt.getopt(sys.argv[1:], 'h',['help', 'path-queue=', 'items=', 'delay=', 'trys=', 'retry=', 'wait=', 'queue-size='])
        except getopt.GetoptError, err:
            print str(err)
            self.usage()
            sys.exit(2)

        for o, a in opts:
            if o in ("-h", "--help"):
                self.usage()
                sys.exit(1)
            elif o in ("--path-queue"): path_queue = a
            elif o in ("--items"): items = int(a)
            elif o in ("--delay"): delay = int(a)
            elif o in ("--trys"): trys = int(a)
            elif o in ("--retry"): retry = int(a)
            elif o in ("--wait"): wait = int(a)
            elif o in ("--queue-size"): limit = int(a)
            else: assert False, "unhandled option"
    
        if not path_queue:
            self.usage()
            sys.exit(2)



    def usage (self):
        print " python " + sys.argv[0] + " <param=value>"
        print 
        print " Params:"
        print
        print " --path-queue : path of the item queue (mandatory)"
        print " --items      : number of processed items per second"
        print " --delay      : time to reload items queue"
        print " --trys       : number of retries in processing an item"
        print " --retry      : the time between retries"
        print " --wait       : time between process items"
        print " --queue-size : the number of items reloaded between delays"
        print



    def sortby(self, list, n):
        list[:] = [(x[n], x) for x in list]
        list.sort()
        list[:] = [val for (key, val) in list]
        return






def main():

    ut = Utils()
    ut.getOptions()
    
    queue = Queue()
    queue.loop()
    

main()




