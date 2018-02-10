import os
import sys
import threading
import httplib
import time
import logging
import random
import pickle


items = 10
wait =  0
delay = 1

path_queue =    "/tmp/damor/queue"

trys = 5
retry = 5
valid_status = [302,200,304]

path_logs =     "/tmp/damor/log"
logfile = path_logs + os.sep + "python_testing.log"

##################################


class ItemTh ( threading.Thread ):

    def __init__(self, item):  
        global logfile

        logging.basicConfig(filename=logfile,level=logging.DEBUG,format='%(asctime)s %(levelname)s %(message)s')
        threading.Thread.__init__(self)  
        self.item = item
                    

    def run ( self ):
        global trys, retry, logfile, path_queue
        
        for x in xrange(trys):
        
            sta = self.process()
            if sta:
                try:
                    os.remove (path_queue + os.sep + self.item)
                except os.error: pass
                sys.exit()
            
            time.sleep (retry)
            self.traza ('Retrying '+ str(x+1) + " of " + str(trys), self.item, "", "")

        #mover a cuarentena o borrar
        self.traza ('The retries are finished' , self.item, "", "")
        try:
            os.remove(path_queue + os.sep + self.item)
        except os.error: pass


    def process (self):
        global path_queue, valid_status

        file = path_queue + os.sep + self.item
        f = open(file , 'r')
        url = (f.readline()).strip()
        f.close()

        sta = 0
        try:
            conn = httplib.HTTPConnection(url)
            conn.request("GET", "")
            r = conn.getresponse()
            #pruebas
            time.sleep(random.randint(1, 10))
            sta = r.status
            data = r.read()
            conn.close()
        except:
            pass
        
        for status in valid_status:
            if status == sta: 
                self.traza ('processed with success', self.item, url, str(sta))
                return sta
        
        self.traza ('processed WITH FAILURE', self.item, url, str(sta))
        return 0

    
    def traza (self, msg, file, url, res):
        logging.debug(self.getName() + ': ' + msg)
        logging.debug(self.getName() + ': ' + 'file: ' + file)
        logging.debug(self.getName() + ': ' + 'url: ' + url)
        logging.debug(self.getName() + ': ' + 'response status: ' + res)




class Queue:

    def __init__(self):  
        global logfile, items

        self.items = items
        self.itemList = []
        logging.basicConfig(filename=logfile,level=logging.DEBUG,format='%(asctime)s %(levelname)s %(message)s')

    def loop (self):
        global items, wait, delay, path_queue, logfile

        driver = Driver()

        while True:
            
            self.itemList.extend ( driver.getItems() )
            
            active = threading.activeCount() - 1
            items_num = len(self.itemList)
            to_launch = self.items if self.items < items_num  else items_num

            logging.debug("# of items en the queue: " + str(items_num))
            logging.debug("active threads: " + str (active)) 
            logging.debug("thrown threads: " + str (to_launch))

            print "# of items en the queue: " + str(items_num)
            print "active threads: " + str (active)
            print "thrown threads: " + str (to_launch)


            for x in xrange ( to_launch ):
                if self.itemList:
                    thr = ItemTh (self.itemList.pop())
                    thr.start()
                time.sleep (wait)

            time.sleep (delay)

        #salio del while: 
        logging.debug("Ended.")
        print "END"
        
    

class Driver:

    def __init__(self): 
        global retry
        self.last_time = time.time()

    
    def getItems (self):
        global path_queue

        items = filter( self.isNewItem  , os.listdir(path_queue))
        
        if items: self.last_time = time.time()
        
        print "LEN " + str( len(items) )
        
        return items

    def isNewItem (self, x):
        try:
            return (os.path.getctime( path_queue + os.sep + x ) > self.last_time)
        except os.error:
            return False

def main():
    
    cola = Queue()
    cola.loop()

main()






