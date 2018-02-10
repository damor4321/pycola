from signal import signal, SIGTERM
import os
import sys
import threading
import httplib
import time
import logging
import Queue
import random
import pickle




# for the server
items = 200
wait =  1
path_queue = "/tmp/damor/queue"

# for the workers
delay = 1
trys =  5
retry = 5
valid_status = [302,200,304]

#logs
path_logs = "/tmp/damor/log"
logfile = path_logs + os.sep + "python_test.log"

Term_=False
queue = Queue.Queue()

class ItemThr(threading.Thread):

    def __init__(self, queue):
        global logfile

        threading.Thread.__init__(self)
        self.queue = queue
        self.retrys = []

        logging.basicConfig(filename=logfile,level=logging.DEBUG,format='%(asctime)s %(levelname)s %(threadName)s %(message)s')


    def run(self):
        global delay, Term_
      

        while not Term_:

            item_from_queue = self.selectNextItem()

            if self.item:
                sta = self.process()
                if sta:
                    try: os.remove (path_queue + os.sep + self.item)
                    except os.error: pass        
                else: 
                    self.addRetry()
                
                if item_from_queue: 
                    #print  self.getName() + " notify it is free"
                    self.queue.task_done()

                time.sleep (delay)
            
        
        
        print self.getName() + " ENDED"

   

    def process (self):
        global path_queue, valid_status

        file = path_queue + os.sep + self.item
        try:
            f = open(file , 'r')
            url = (f.readline()).strip()
            f.close()
        except: return 0

        sta = 0
        try:
            conn = httplib.HTTPConnection(url)
            conn.request("GET", "/index.html")
            r = conn.getresponse()
            #testing
            #time.sleep(random.randint(1, 10))
            sta = r.status
            #data = r.read()
            conn.close()
        except:
            pass
        
        for status in valid_status:
                if status == sta: 
                    self.trace ('processed with success', self.item, url, str(sta))
                    return sta

        self.trace ('processed WITH FAILURE', self.item, url, str(sta))
        return 0



    def addRetry (self):
        global trys, touch

        #print self.getName() + ":addRetry: size of the retry list :" + str(len(self.retrys))

        for re in self.retrys: 

            if re.item == self.item:
                if trys > re.count: 
                    re.inc()
                    #print self.getName() + ":addRetry: retries increase " +self.item + " to retries="+ str (re.count)
                    self.trace (':addRetry: retries increase, retries=' + str (re.count), self.item, "", "")
                    return
                
                # It is sent to quarantine or is deleted
                #print self.getName() + ":addRetry: deleted because reaching the maximum of retries " + self.item
                self.trace (':addRetry: Reached the maximum number of retries.', self.item, "", "")
                self.retrys.remove(re)
                try: os.remove(path_queue + os.sep + self.item)
                except os.error: pass   
                return
           
        # It is new in retries
        #print self.getName() + ":addRetry: add to " +self.item + " retries"
        self.retrys.append(Retry(self.item))



    def selectNextItem (self):
        global retry
      

        for re in self.retrys:
            if (re.last_time + retry) < time.time(): 
                self.item = re.item
                #print self.getName() + ":selectNextItem: the item is picked from the list of retries for being its time. item: " + self.item
                return False
      
        try:
        	self.item = self.queue.get(False)
        	#print self.getName() + ":selectNextItem: the item is picked from the queue: " + self.item
        	return True
        except Queue.Empty: pass

        self.item = ""
        return False



    def trace (self, msg, file, url, res):
        logging.debug( ': ' + msg)
        logging.debug( ': ' + 'file: ' + file)
        logging.debug( ': ' + 'url: ' + url)
        logging.debug( ': ' + 'response status: ' + res)





class Server:

    def __init__(self):  
        pass
    

    def startWorkers (self, items):                

        for x in xrange ( items ):
            worker = ItemThr(queue)
            worker.setDaemon(True)
            worker.start()


    def loop (self):
        global wait, path_queue, logfile


        driver = Driver()
        Items = driver.getInitialItems()
        logging.debug("new items in the list: " + str ( len(Items) ))
        print "new items in the list: " + str (len(Items))
       
        while True:

            if Items:
                for item in Items: queue.put(item)

            Items = driver.getItems()

            logging.debug("new items in the list: " + str ( len(Items) ))
            print "new items in the list: " + str (len(Items))

            time.sleep (wait)

        
        #go out of while:
        #wait on the queue until everything has been processed     
        self.terminate()
        logging.debug("Ended.")
        print "END"
 

    def terminate(self):
        global Term_

        print "TERMINATE was CALLED"
        Term_ = True

        #queue.join()

        for t in threading.enumerate():
            if t is  threading.currentThread(): continue
            t.join()
        
        sys.exit()


class Driver:

    def __init__(self): 
        global retry
        self.last_time = time.time()

    
    def getItems (self):
        global path_queue

        items = filter( self.isNewItem , os.listdir(path_queue))        
        if items:
            self.last_time = time.time()
        
        return items


    def getInitialItems (self):
        global path_queue
        
        items = os.listdir(path_queue)
        self.last_time = time.time()
       
        #Beacuse this method only runs the first time, we sleep, so that getItems have the possibility to obtain items
        time.sleep(2)

        return items

    def isNewItem (self, x):
        try:
            return (os.path.getctime( path_queue + os.sep + x ) > self.last_time)
        except os.error:
            return False




class Retry:
    
    def __init__ (self, item):
        self.item = item
        self.count = 1
        self.last_time = time.time()
    
    def inc (self):
        self.count = self.count + 1
        self.last_time = time.time()
    
    def __toString(self): 
        return self.count



def main():

    try:
        cola = Server()
        cola.startWorkers(items)
        cola.loop()

    except KeyboardInterrupt:
        cola.terminate()

main()


