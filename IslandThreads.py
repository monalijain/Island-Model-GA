__author__ = 'Monali'

import threading
import time
import IslandParameters as ip
from DB_DBUtils import *
from DBUtils_Databases import *
class myThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def run(self):
        print "Starting " + self.name

        # Get lock to synchronize threads
        #threadLock.acquire()

        dbObject=Diff_DBUtils()
        dbObject.dbConnect("island"+str(self.threadID))
        dbObject.dbQuery("DELETE FROM island_generation_table")
        dbObject.dbQuery("DELETE FROM island_migration_performance_measures")
        dbObject.dbQuery("DELETE FROM island_migration_pop_table")
        dbObject.dbQuery("DELETE FROM island_pareto_front_table")
        dbObject.dbClose()

        # Free lock to release next thread
        threadLock.release()

        print_time(self.name, self.threadID)


        print "Ending " + self.name

def print_time(threadName, threadID):
    for i in range(0,5):
        time.sleep(threadID)
        print threadName+" "

if __name__ == '__main__':
    threadLock = threading.Lock()
    threads = []
    threadList = []
    threadNum = 1

    #CREATING THREADLIST
    for i in range(ip.NumSubpop):
        threadList.append("Thread" + str(threadNum))
        threadNum += 1

    #START
    threadNum=1
    for tName in threadList:
        threadObj = myThread(threadNum,tName)
        threadObj.start()
        threads.append(threadObj)
        threadNum += 1

    #dbObject.dbClose()
    # Wait for all threads to complete
    for t in threads:
        t.join()
    print "Exiting Main Thread"