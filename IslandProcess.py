__author__ = 'Monali'

from multiprocessing import Process, Lock
import IslandParameters as ip
from DBUtils_Databases import *
import GlobalVariables as gv
import csv
from WFList import CreateWFList
from NonSortedGANewVersion import *
from DB_MakePerf import Make_Performance_Measures
from Migrate import Migrate
from Setup_Islands import *

def startProcess(lock, processID, pName, WalkNo, Epochs, initial_individualID,initial_generation, MaxGen, MaxIndividuals, TBeginList, TendList):

    print "Starting " + pName

    dbObject=Diff_DBUtils()
    dbObject.dbConnect("island"+str(processID))

    processmyThread(pName, processID, WalkNo, MaxIndividuals, initial_individualID, initial_generation, MaxGen, dbObject, TBeginList, TendList)
    createMigratingPopulation(pName, processID, MaxIndividuals, Epochs, WalkNo, dbObject)

    print "Ending " + pName

def processmyThread(pName, processID, walkforward_number, MaxIndividuals, initial_individualID,initial_generation ,MaxGen, dbObject, TBeginList,TendList):
    walkNo=walkforward_number-1
    print "processmyThread, ", processID
    stringNetPLRatio="NetPLRatio"+str(walkforward_number)+ "_" + "stock"+ str(gv.stock_number)+"island"+str(processID)+".csv"
    PLRatio=csv.writer(open(stringNetPLRatio,'a'))

    print("Calculating Performance Measures of Training Period for walkforward ",walkforward_number)
    Make_Performance_Measures(TBeginList[walkNo],TendList[walkNo],Train_Or_Report="Training",walkforward_number=walkforward_number,islandID=processID, dbObject1=dbObject)

    print("Calling NonSorted Genetic Algorithm")
    [A,C,StoreParetoID]=NonSortedGA(initial_individualID, initial_generation, walkforward_number, gv.MaxIndividualsInGen, MaxGen, MaxIndividuals, processID, dbObject)
    #[A,B,C]=NonSortedGA("Tradesheets","PriceSeries",6,'20120622','20121109','20121112','20130111',0.0,2,12)

    for key in C.keys():
        PLRatio.writerow([key,C[key][0],C[key][1]])

    PLRatio.writerow([])

def createMigratingPopulation(threadName, threadID, populationSize, Epochs, walkFwd,dbObject):

    dbObject.dbQuery("DELETE FROM island_migration_id_table")
    dbObject.dbQuery("DELETE FROM island_migration_pop_table")
    dbObject.dbQuery("DELETE FROM island_migration_performance_measures")

    before_individuals=Epochs*(ip.LengthEpoch)*gv.MaxIndividualsInGen
    print "before_individuals of "+threadName+" = ",before_individuals
    after_individuals=populationSize-before_individuals
    print "after_individuals of "+threadName+" = ",after_individuals

    dbObject.dbQuery("INSERT INTO island_migration_id_table (individual_id, island_id) (SELECT individual_id, "+str(threadID)+" FROM island_id_table WHERE dummy_id > "+str(before_individuals)+" ORDER BY RAND() LIMIT "+str(int(ip.MigrantSize*after_individuals))+")")
    dbObject.dbQuery("INSERT INTO island_migration_pop_table (SELECT island_population_table.* FROM island_population_table INNER JOIN island_migration_id_table ON island_population_table.individual_id  = island_migration_id_table.individual_id ORDER BY island_population_table.trade_id)")
    dbObject.dbQuery("DELETE island_population_table.* FROM island_population_table INNER JOIN island_migration_id_table ON island_population_table.individual_id  = island_migration_id_table.individual_id")

    string_1="performance_measures_training_walk"+str(walkFwd)+"_stock"+str(gv.stock_number)
    dbObject.dbQuery("INSERT INTO island_migration_performance_measures (SELECT "+string_1+".* FROM "+string_1+" INNER JOIN island_migration_id_table ON "+string_1+".individual_id  = island_migration_id_table.individual_id ORDER BY island_migration_id_table.individual_id)")
    dbObject.dbQuery("DELETE "+string_1+".* FROM "+string_1+" INNER JOIN island_migration_id_table ON "+string_1+".individual_id  = island_migration_id_table.individual_id")

    dbObject.dbQuery("DELETE island_id_table.* FROM island_id_table INNER JOIN island_migration_id_table ON island_id_table.individual_id = island_migration_id_table.individual_id")


if __name__ == '__main__':
    lock = Lock()

    activeProcess = []
    processList = []

    #Create WALKFWD LIST:
    [TBeginList,TendList,RBeginList,REndList]=CreateWFList(gv.priceSeriesTable, gv.numDaysInTraining, gv.numDaysInReporting) #Creates Walkforward List
    print(TBeginList,TendList)

    #CREATING PROCESS LIST
    processNum = 1
    for i in range(0,ip.NumSubpop):
        processList.append("Process" + str(processNum))
        dbObject=Diff_DBUtils()
        dbObject.dbConnect("island"+str(processNum))
        dbObject.dbQuery("DELETE FROM island_generation_table")
        dbObject.dbQuery("DELETE FROM island_pareto_front_table")
        dbObject.dbQuery("DELETE FROM performance_measures_training_walk1_stock1")
        dbObject.dbClose()
        processNum += 1


    WalkNo=1
    subPopSize=Setup_Islands(WalkNo)
    Epochs=1
    initial_generation=1
    initial_individual_id=(initial_generation-1)*gv.MaxIndividualsInGen+1
    MaxGen=(Epochs)*ip.LengthEpoch
    MaxIndividuals= subPopSize

    #STARTING PROCESSES AND STORING THEM INTO ACTIVE PROCESSES
    processNum=1
    for pName in processList:
        pObj=Process(target=startProcess, args=(lock, processNum, pName, WalkNo, Epochs, initial_individual_id,initial_generation, MaxGen, MaxIndividuals, TBeginList, TendList))
        pObj.start()
        activeProcess.append(pObj)
        processNum += 1

    # Wait for all processes to complete
    for p in activeProcess:
        p.join()


    last_individual_id=0

    NumEpochs=subPopSize/(ip.LengthEpoch*gv.MaxIndividualsInGen)

    activeProcess=[]

    for num in range(1,NumEpochs):
        Migrate(walkFwd=WalkNo)

        Epochs=num+1
        initial_generation=(Epochs-1)*ip.LengthEpoch
        initial_individual_id=(initial_generation-1)*gv.MaxIndividualsInGen+1
        MaxGen=(Epochs)*ip.LengthEpoch

        #STARTING PROCESSESS AND STORING THEM IN LIST
        processNum=1
        for pName in processList:
            pObj=Process(target=startProcess, args=(lock, processNum, pName, WalkNo, Epochs, initial_individual_id, initial_generation, MaxGen, MaxIndividuals, TBeginList, TendList))
            pObj.start()
            activeProcess.append(pObj)
            processNum += 1

        # Wait for all threads to complete
        for p in activeProcess:
            p.join()

    print "Exiting Main Thread"
