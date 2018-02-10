import os
import sys
import threading
import httplib
import time
import logging
import random
import pickle




items = 200
wait =  0
delay = 1

path_queue =    "/tmp/damor/queue3"
path_retries =  "/tmp/damor/trys"

trys = 5
retry = 5
valid_status = [302,200,304]

path_logs =     "/tmp/damor/log"
logfile = path_logs + os.sep + "python_testing.log"


##################################


class ItemTh ( threading.Thread ):

    def __init__(self, item):  
        global logfile
        
        logging.basicConfig(filename=logfile,level=logging.DEBUG,format='%(asctime)s %(levelname)s %(threadname)s %(message)s')
        threading.Thread.__init__(self)  
        self.item = item
                    
    

    def run ( self ):
        global trys, retry, logfile, path_queue
        
        sta = self.process()

        if sta:
            try: 
                os.remove (path_queue + os.sep + self.item)
                os.remove (path_retries + os.sep + self.item)
            except os.error: pass
            self.traza ('processed with success', self.item, self.url, str(sta))
            sys.exit()
        
        self.retry()
        self.traza ('processed WITH FAILURE', self.item, self.url, str(sta))


    def process (self):
        global path_queue, valid_status

        self.url = ""        
        file = path_queue + os.sep + self.item
        try:
            f = open(file , 'r')
            self.url = (f.readline()).strip()
            f.close()
        except: return 0

        sta = 0
        try:
            conn = httplib.HTTPConnection(self.url)
            conn.request("GET", "/index.html")
            r = conn.getresponse()
            # testing
            #time.sleep(random.randint(1, 10))
            sta = r.status
            #data = r.read()
            conn.close()
        except: pass
        
        for status in valid_status:
                if status == sta: return sta
        
        return 0


    def retry (self):

        global path_retries, trys, touch

        file = path_queue + os.sep + self.item
        file_ret = path_retries + os.sep + self.item
        
        re = ""
        if os.path.isfile(file_ret):
            try:
                f = open(file_ret, 'r')
                re = pickle.load(f)
                f.close()
            except: return #apart from the file the f**** pickle can crack: EOFError

        if re != "": re.inc()
        else: re = Retry()
            

        if trys == re.count:
            #It is sent to quarantine or is deleted but in any case it is removed from there
            try:
                os.remove (file)
                os.remove (file_ret)
            	self.traza ('The retries are finished' , file, self.url, "FAIL")
            except os.error: pass
           
        else:
            try:
                f = open(file_ret, 'wb')
                pickle.dump(re, f)
                f.close()
            except: return
            self.traza (str(trys - re.count) + ' retries left' , file, self.url, "FAIL")
            
            #if os.path.isfile(file): 
            #    try:
            #        touch(file)
            #    except:
            #        try:
            #            os.remove (file_ret)
            #        except os.error: return



    def traza (self, msg, file, url, res):
        logging.debug(': ' + msg)
        logging.debug(': ' + 'file: ' + file)
        logging.debug(': ' + 'url: ' + url)
        logging.debug(': ' + 'response status: ' + res)




class Queue:

    def __init__(self):  
        global logfile, items
        
        self.items = items
        logging.basicConfig(filename=logfile,level=logging.DEBUG,format='%(asctime)s %(levelname)s %(message)s')

    def loop (self):
        global items, wait, delay, path_queue, logfile
        
        driver = Driver()
        Items = driver.getInitialItems()
        logging.debug("# of items in the queue: " + str (len(Items))) 
        print "# of items in the queue: " + str (len(Items))


        while True:

            active = threading.activeCount() - 1
            to_launch = self.items - active
            
            logging.debug("# total: " + str (items) + "| # active: " + str (active) + "| # to threw: " + str (to_launch))
            print "# total: " + str (items) + "| # active: " + str (active) + "| # to threw: " + str (to_launch)


            for x in xrange ( to_launch ):
                if Items:
                    item = Items.pop(0) 
                    thr = ItemTh (item)
                    thr.start()
                time.sleep (wait)

            Items.extend(driver.getItems())
            logging.debug("# of items in the queue: " + str (len(Items))) 
            print "# of items in the queue: " + str (len(Items))

            time.sleep (delay)

        #out of the while: the children die alone, there is no need for a terminate them.
        logging.debug("Ended.")
        print "END"
        
    

class Driver:

    def __init__(self): 
        global retry
        self.last_time = time.time()


    def getInitialItems (self):
        global path_queue
        
        items = os.listdir(path_queue)
        self.last_time = time.time()
        time.sleep(2)
        return items

    
    def getItems (self):
        global path_queue

        all_items = os.listdir(path_queue)
        items = filter(self.getRetries , all_items)

        print "# of items of retries: " + str (len(items))
        
        new_items = filter(self.isNewItem , os.listdir(path_queue))
        items.extend(new_items)

        #open("./aaa", 'a').close()
        #self.last_time = os.path.getctime( "./aaa")
        #os.remove("./aaa")

        self.last_time = time.time()
        print "# of items of new_items: " + str (len(new_items))
        print "# of items by sum: " + str (len(items))
        
        return items


    def getRetries (self, item):
        global path_retries, retry

        loc = path_retries + os.sep + item
       
        if os.path.isfile(loc):
            try:
                f = open(loc, 'r')
                re = pickle.load(f)
                f.close()
                return (re.last_time < (time.time() - retry))
            except: pass
        return False

    
    def isNewItem (self, item):
        try:
            return (os.path.getctime( path_queue + os.sep + item ) > self.last_time)
        except os.error: return False



class Retry:
    
    def __init__ (self): 
        self.count = 1
        self.last_time = time.time()
    
    def inc (self):
        self.count = self.count + 1
        self.last_time = time.time()
    
    def __toString(self): 
        return self.count




def touch(fname):
    open(fname, 'a').close()
    os.utime(fname, None)


def main():
    
    cola = Queue()
    cola.loop()

main()


