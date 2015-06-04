__author__ = 'Monali'

__author__ = 'Monali'

import threading
import time
import random
import IslandParameters as ip
from DB_DBUtils import *
from DBUtils_Databases import *
import logging
import csv
import time
from datetime import timedelta, datetime
import numpy as np
#from Setup_Islands import *
from WFList import CreateWFList
from NonSortedGANewVersion import *
import GlobalVariables as gv
from DB_MakePerf import Make_Performance_Measures
from Migrate import Migrate

import mysql.connector.pooling


class newThread (threading.Thread):
    def __init__(self, threadID, name, subPopSize,WalkNo,Epochs,initial_individual_id,initial_generation):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.subPopSize = subPopSize
        self.WalkNo=WalkNo
        self.Epochs=Epochs
        self.initial_individual_id=initial_individual_id
        self.initial_generation=initial_generation

    def run(self):
        print "Starting " + self.name

        MaxGen=(self.subPopSize)/gv.MaxIndividualsInGen

        dbObject=Diff_DBUtils()
        dbObject.dbConnect("island"+str(self.threadID))

        logging.info("Calling Process My Thread function with Thread= "+str(self.threadID)+" MaxGen = "+str(self.Epochs*ip.LengthEpoch)+"generation = "+str(self.initial_generation)+" initial_individual_id = "+str(self.initial_individual_id))
        processnewThread(self.name,self.threadID,MaxGen=(self.Epochs)*ip.LengthEpoch,walkforward_number=self.WalkNo,MaxIndividuals=self.subPopSize,initial_individualID=self.initial_individual_id,generation=self.initial_generation)

        logging.info("Calling CreateMigrating Population, with Epochs = "+str(self.Epochs))
        createMigratingPopulation(self.name,self.threadID,self.subPopSize,self.Epochs,self.WalkNo)

        print "Ending " + self.name


def processnewThread(threadName, threadID, MaxGen,walkforward_number,MaxIndividuals,initial_individualID,generation):
    walkNo=walkforward_number-1

    stringNetPLRatio="NetPLRatio"+str(walkforward_number)+ "_" + "stock"+ str(gv.stock_number)+"island"+str(threadID)+".csv"
    PLRatio=csv.writer(open(stringNetPLRatio,'a'))
    NGA=NonSortedGANewVersion()
    logging.info("Calling NonSorted Genetic Algorithm")
    [A,C,StoreParetoID]=NGA.NonSortedGA(initial_individualID,generation,walkforward_number,gv.MaxIndividualsInGen,MaxGen,MaxIndividuals,threadID)
    #[A,B,C]=NonSortedGA("Tradesheets","PriceSeries",6,'20120622','20121109','20121112','20130111',0.0,2,12)

    for key in C.keys():
        PLRatio.writerow([key,C[key][0],C[key][1]])

    PLRatio.writerow([])


class myThread (threading.Thread):
    def __init__(self, threadID, name, subPopSize,WalkNo,Epochs,initial_individual_id,initial_generation,dbObject):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.subPopSize = subPopSize
        self.WalkNo=WalkNo
        self.Epochs=Epochs
        self.initial_individual_id=initial_individual_id
        self.initial_generation=initial_generation
        self.dbObject=dbObject

    def run(self):
        print "Starting " + self.name
        logging.info("DELETING THE TABLES of ISLAND")
        self.dbObject.execute("DELETE FROM island_generation_table")
        self.dbObject.execute("DELETE FROM island_pareto_front_table")
        self.dbObject.execute("DELETE FROM performance_measures_training_walk1_stock1")
        create(self.name, self.threadID, self.subPopSize, self.Epochs, self.WalkNo,self.dbObject)

    '''
        logging.info("Calling Process My Thread function with Thread= "+str(self.threadID)+" MaxGen = "+str(self.Epochs*ip.LengthEpoch)+"generation = "+str(self.initial_generation)+" initial_individual_id = "+str(self.initial_individual_id))
        processmyThread(self.name,self.threadID,MaxGen=(self.Epochs)*ip.LengthEpoch,walkforward_number=self.WalkNo,MaxIndividuals=self.subPopSize,initial_individualID=self.initial_individual_id,generation=self.initial_generation)

        logging.info("Calling CreateMigrating Population, with Epochs = "+str(self.Epochs))
    '''

def create(threadName, threadID, populationSize, Epochs, walkFwd,dbObject):
    '''
    dbObject=Diff_DBUtils()
    dbObject.dbConnect("island"+str(threadID))
    '''
    dbObject.execute("DELETE FROM island_migration_id_table")
    dbObject.execute("DELETE FROM island_migration_pop_table")
    dbObject.execute("DELETE FROM island_migration_performance_measures")

    before_individuals=Epochs*(ip.LengthEpoch)*gv.MaxIndividualsInGen
    print "before_individuals = ",before_individuals
    after_individuals=populationSize-before_individuals

    dbObject.execute("INSERT INTO island_migration_id_table (individual_id, island_id) SELECT individual_id, "+str(threadID)+" FROM island_id_table ORDER BY RAND() LIMIT "+str(int(ip.MigrantSize*after_individuals))+" OFFSET "+str(before_individuals))
    dbObject.execute("INSERT INTO island_migration_pop_table (SELECT island_population_table.* FROM island_population_table INNER JOIN island_migration_id_table ON island_population_table.individual_id  = island_migration_id_table.individual_id ORDER BY island_population_table.trade_id)")
    dbObject.execute("DELETE island_population_table.* FROM island_population_table INNER JOIN island_migration_id_table ON island_population_table.individual_id  = island_migration_id_table.individual_id")

    string_1="performance_measures_training_walk"+str(walkFwd)+"_stock"+str(gv.stock_number)
    dbObject.execute("INSERT INTO island_migration_performance_measures (SELECT "+string_1+".* FROM "+string_1+" INNER JOIN island_migration_id_table ON "+string_1+".individual_id  = island_migration_id_table.individual_id ORDER BY island_migration_id_table.individual_id)")
    dbObject.execute("DELETE "+string_1+".* FROM "+string_1+" INNER JOIN island_migration_id_table ON "+string_1+".individual_id  = island_migration_id_table.individual_id")

    dbObject.execute("DELETE island_id_table.* FROM island_id_table INNER JOIN island_migration_id_table ON island_id_table.individual_id = island_migration_id_table.individual_id")


'''
    def run(self):
        print "Starting " + self.name

        # Get lock to synchronize threads
        #threadLock.acquire()

        file="island"+str(self.threadID)+".log"
        logging.basicConfig(filename=file,level=logging.DEBUG)


        dbObject=Diff_DBUtils()
        dbObject.dbConnect("island"+str(self.threadID))

        logging.info("DELETING THE TABLES of ISLAND")
        self.dbObject.dbQuery("DELETE FROM island_generation_table")
        self.dbObject.dbQuery("DELETE FROM island_pareto_front_table")
        self.dbObject.dbQuery("DELETE FROM performance_measures_training_walk1_stock1")


        logging.info("Calling Process My Thread function with Thread= "+str(self.threadID)+" MaxGen = "+str(self.Epochs*ip.LengthEpoch)+"generation = "+str(self.initial_generation)+" initial_individual_id = "+str(self.initial_individual_id))
        processmyThread(self.name,self.threadID,MaxGen=(self.Epochs)*ip.LengthEpoch,walkforward_number=self.WalkNo,MaxIndividuals=self.subPopSize,initial_individualID=self.initial_individual_id,generation=self.initial_generation)

        logging.info("Calling CreateMigrating Population, with Epochs = "+str(self.Epochs))
        createMigratingPopulation(self.name, self.threadID, self.subPopSize, self.Epochs, self.WalkNo,self.dbObject)

        # Free lock to release next thread
        #threadLock.release()

        print "Ending " + self.name
        '''

def createMigratingPopulation(threadName, threadID, populationSize, Epochs, walkFwd,dbObject):
    '''
    dbObject=Diff_DBUtils()
    dbObject.dbConnect("island"+str(threadID))
    '''
    dbObject.dbQuery("DELETE FROM island_migration_id_table")
    dbObject.dbQuery("DELETE FROM island_migration_pop_table")
    dbObject.dbQuery("DELETE FROM island_migration_performance_measures")

    before_individuals=Epochs*(ip.LengthEpoch)*gv.MaxIndividualsInGen
    print "before_individuals = ",before_individuals
    after_individuals=populationSize-before_individuals

    dbObject.dbQuery("INSERT INTO island_migration_id_table (individual_id, island_id) SELECT individual_id, "+str(threadID)+" FROM island_id_table ORDER BY RAND() LIMIT "+str(int(ip.MigrantSize*after_individuals))+" OFFSET "+str(before_individuals))
    dbObject.dbQuery("INSERT INTO island_migration_pop_table (SELECT island_population_table.* FROM island_population_table INNER JOIN island_migration_id_table ON island_population_table.individual_id  = island_migration_id_table.individual_id ORDER BY island_population_table.trade_id)")
    dbObject.dbQuery("DELETE island_population_table.* FROM island_population_table INNER JOIN island_migration_id_table ON island_population_table.individual_id  = island_migration_id_table.individual_id")

    string_1="performance_measures_training_walk"+str(walkFwd)+"_stock"+str(gv.stock_number)
    dbObject.dbQuery("INSERT INTO island_migration_performance_measures (SELECT "+string_1+".* FROM "+string_1+" INNER JOIN island_migration_id_table ON "+string_1+".individual_id  = island_migration_id_table.individual_id ORDER BY island_migration_id_table.individual_id)")
    dbObject.dbQuery("DELETE "+string_1+".* FROM "+string_1+" INNER JOIN island_migration_id_table ON "+string_1+".individual_id  = island_migration_id_table.individual_id")

    dbObject.dbQuery("DELETE island_id_table.* FROM island_id_table INNER JOIN island_migration_id_table ON island_id_table.individual_id = island_migration_id_table.individual_id")


def processmyThread(threadName, threadID, MaxGen,walkforward_number,MaxIndividuals,initial_individualID,generation):
    walkNo=walkforward_number-1
    print "processmyThread, ", threadID
    stringNetPLRatio="NetPLRatio"+str(walkforward_number)+ "_" + "stock"+ str(gv.stock_number)+"island"+str(threadID)+".csv"
    PLRatio=csv.writer(open(stringNetPLRatio,"wb"))
    NGA=NonSortedGANewVersion()
    print("Calculating Performance Measures of Training Period for walkforward %s",walkforward_number)
    Make_Performance_Measures(TBeginList[walkNo],TendList[walkNo],Train_Or_Report="Training",walkforward_number=walkforward_number,islandID=threadID)

    print("Calling NonSorted Genetic Algorithm")
    [A,C,StoreParetoID]=NGA.NonSortedGA(initial_individualID,generation,walkforward_number,gv.MaxIndividualsInGen,MaxGen,MaxIndividuals,threadID)
    #[A,B,C]=NonSortedGA("Tradesheets","PriceSeries",6,'20120622','20121109','20121112','20130111',0.0,2,12)


    for key in C.keys():
        PLRatio.writerow([key,C[key][0],C[key][1]])

    PLRatio.writerow([])



if __name__ == '__main__':
    threadLock = threading.Lock()
    threads = []
    threadList = []

    [TBeginList,TendList,RBeginList,REndList]=CreateWFList(gv.priceSeriesTable, gv.numDaysInTraining, gv.numDaysInReporting) #Creates Walkforward List
    print(TBeginList,TendList)

    WalkNo=1
    #subPopSize=Setup_Islands(WalkNo)
    subPopSize=100

    #CREATING THREADLIST
    threadNum = 1
    for i in range(ip.NumSubpop):
        threadList.append("Thread" + str(threadNum))
        threadNum += 1



    dbconfig = {
      "database": "island1",
      "user":     "root",
      "password": "controljp"
    }

    cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name = "mypool",
                                                          pool_size = 3,
                                                          **dbconfig)
    cnx1 = cnxpool.get_connection()
    cnx2 = cnxpool.get_connection()

    dbconfig2 = {
      "database": "island2",
      "user":     "root",
      "password": "controljp"
    }

    cnxpool2 = mysql.connector.pooling.MySQLConnectionPool(pool_name = "mypool2",
                                                          pool_size = 3,
                                                          **dbconfig2)

    bnx1=cnxpool2.get_connection()



    #STARTING THREADS AND STORING THEM IN THREADS
    threadNum=1
    for tName in threadList:
        #dbObject=Diff_DBUtils()
        #dbObject.dbConnect("island"+str(threadNum))
        if(threadNum==1):
            dbObject=cnx1.cursor()
        else:
            dbObject=bnx1.cursor()
        threadObj = myThread(threadNum,tName,subPopSize,WalkNo,Epochs=1,initial_individual_id=0,initial_generation=1,dbObject=dbObject)
        threadObj.start()
        time.sleep(5)
        threads.append(threadObj)
        threadNum += 1

    last_individual_id=0

    # Wait for all threads to complete
    for t in threads:
        t.join()

    cnx1.close()
    bnx1.close()
    cnx2.close()

    NumEpochs=subPopSize/(ip.LengthEpoch*gv.MaxIndividualsInGen)

    for num in range(1,NumEpochs):
        Migrate(walkFwd=WalkNo)

        Epochs=num+1
        initial_individual_id=last_individual_id+(ip.LengthEpoch*gv.MaxIndividualsInGen)
        initial_generation=num*ip.LengthEpoch
        threads = []
        #STARTING THREADS AND STORING THEM IN THREADS
        threadNum=1
        for tName in threadList:
            threadObj = newThread(threadNum,tName,subPopSize,WalkNo,Epochs,initial_individual_id,initial_generation)
            threadObj.start()
            threads.append(threadObj)
            threadNum += 1

        # Wait for all threads to complete
        for t in threads:
            t.join()

        last_individual_id=initial_individual_id

    print "Exiting Main Thread"

