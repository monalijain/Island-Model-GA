from PerformanceMeasures_ShortVersion import *
from MakeNewPopulationVersion2 import MakeNewPopulation
import csv
from datetime import datetime
from FastNonDominatedSort import FastNonDominatedSort
import GlobalVariables as gv
import logging
from DBUtils_Databases import *

C={}
def NonSortedGA(initial_individualID,generation,walkforward_number,MaxIndividualsInGen,MaxGen,MaxIndividuals,islandID, dbObject):
    #For checking the convergence of the Pareto Optimal Front
    ParetoOptimalFront={}
    print("initial_individualID = ",initial_individualID)
    print("generation = ",generation)

    StoreParetoID={}
    Pt={}
    logging.info("Current Time %s", str(datetime.now()))
    PerfObj=PerformanceMeasures()
    for num in range(initial_individualID,initial_individualID+MaxIndividualsInGen):
        PerfM=PerfObj.CalculatePerformanceMeasures(num,walkforward_number,gv.stock_number,dbObject)
        Pt[num]=[PerfM[0][0],PerfM[0][1],PerfM[0][2],PerfM[0][3],PerfM[0][4],PerfM[0][5],PerfM[0][6]]

    #print Pt, "threadID", islandID

    #print Pt
    logging.info("We have Initial seed individuals")

    for gen in range(generation,min(MaxGen,MaxIndividuals/MaxIndividualsInGen)):
        print gen," generation"
        if(gen==generation and gen!=1):
            print("Initial population should not be inserted")
        else:
            for indiv in Pt.keys():
                dbObject.dbQuery("INSERT INTO island_generation_table (generation, individual_id) VALUES (" + str(gen) + ", " + str(indiv) + ")")

        StoreParetoID_EachGen=[]
        logging.info("This is generation %s",gen)
        logging.info("Current Time %s",str(datetime.now()))
        logging.info("Non Dominated Sorting of generation %s",gen)
        F=FastNonDominatedSort(Pt) #Create Fronts From Population
        logging.info("Pareto Optimal Front of generation %s is: %s",gen,F[1])

        if(gen==generation and gen!=1):
            print("Initial pareto population should not be inserted")
        else:
            for key in F[1].keys():
                StoreParetoID_EachGen.append(key)
                dbObject.dbQuery("INSERT INTO island_pareto_front_table " \
                                           "(generation, individual_id) " \
                                         "VALUES " \
                                         "("+str(gen) + ", " + str(key) + ")")
                #print key, "Pareto Front KEYS", islandID

        StoreParetoID[gen]=StoreParetoID_EachGen
        i=max(F.keys())
        #if(bool(F[2])):
        #    lengOfFront1and2=len(F[1])+len(F[2])
        #else:
        #    lengOfFront1and2=len(F[1])

        lengOfFront1and2=len(F[1])

        logging.info("Making New Population")
        Q=MakeNewPopulation(F,Pt,i,gen,MaxIndividualsInGen,lengOfFront1and2)
        Qt={}
        counter=0
        for num in Q:
            PerfM=PerfObj.CalculatePerformanceMeasures(num,walkforward_number,gv.stock_number,dbObject)
            Qt[num]=[PerfM[0][0],PerfM[0][1],PerfM[0][2],PerfM[0][3],PerfM[0][4],PerfM[0][5],PerfM[0][6]]
            counter =counter +1
        logging.info("Number of children in generation %s is %s",gen,counter)
        #Pt1=dict((F[1]).items()+(F[2]).items()+Qt.items())
        Pt1=dict((F[1]).items()+Qt.items())

        logging.info("Checking Convergence")
        if(CheckConvergenceOfPopulation(F,gen)==0):
            logging.info("Converged")
        Pt=Pt1

    gen=min(MaxGen,MaxIndividuals/MaxIndividualsInGen)
    print gen, "generation"
    for indiv in Pt.keys():
        dbObject.dbQuery("INSERT INTO island_generation_table " \
                                         "(generation, individual_id) " \
                                         "VALUES " \
                                         "(" + str(gen) + ", " + str(indiv) + ")")

    StoreParetoID_EachGen=[]
    F=FastNonDominatedSort(Pt)

    NetPL_CurrentGen=0.0
    TotalTrades_CurrentGen=0

    logging.info("Calculating NetPL/Total Trades of Pareto Optimal Front")
    for individual in F[1].keys():
        NetPL_CurrentGen=float(F[1][individual][4])+NetPL_CurrentGen
        TotalTrades_CurrentGen=float(F[1][individual][5])+TotalTrades_CurrentGen
        StoreParetoID_EachGen.append(individual)
        dbObject.dbQuery("INSERT INTO island_pareto_front_table " \
                                     "(generation, individual_id) " \
                                     "VALUES " \
                                     "(" + str(gen) + ", " + str(individual) + ")")
            #print(individual)

    StoreParetoID[gen]=StoreParetoID_EachGen

    if(TotalTrades_CurrentGen==0):
        C[gen]=[-50000,0]
    else:
        C[gen]=[NetPL_CurrentGen/(1.0*TotalTrades_CurrentGen),TotalTrades_CurrentGen]
    logging.info("NetPL/TotalTrades[gen=%s] = %s",gen,C[gen])


    #print "Performance Measure of evolved individuals in Training Period",Pt
    logging.info("Returning From NonSorted GA New Version for walkforward %s", walkforward_number)
    #print StoreParetoID

    return [Pt,C,StoreParetoID]


def CheckConvergenceOfPopulation(F,gen):
    NetPL_CurrentGen=0.0
    TotalTrades_CurrentGen=0

    logging.info("Calculating NetPL/Total Trades of Pareto Optimal Front")

    for individual in F[1].keys():
        NetPL_CurrentGen=float(F[1][individual][4])+NetPL_CurrentGen
        TotalTrades_CurrentGen=float(F[1][individual][5])+TotalTrades_CurrentGen

    if(TotalTrades_CurrentGen==0):
        C[gen]=[-50000,0]
    else:
        C[gen]=[NetPL_CurrentGen/(1.0*TotalTrades_CurrentGen),TotalTrades_CurrentGen]
    logging.info("NetPL/TotalTrades[gen=%s] is: %s",gen,C[gen])
    '''
    if(gen==1):
        return 1
    else:
        ParetoOptimalFront[gen]=abs(C[gen][0]-C[gen-1][0])

    #print "ParetoOptimalFront[",gen,"]:",ParetoOptimalFront
    if(gen<max(gv.CheckGen,gv.MinimumGen)):
        return 1
    done=0
    for i in range(gen-gv.CheckGen+1,gen+1):
        if(ParetoOptimalFront.has_key(i)):
            if(ParetoOptimalFront[i]<=gv.ConvergenceValue):
                done+=1
    if(done==gv.CheckGen):
        logging.info("Converged algorithm in generation %s",gen)
        return 0
    '''
    return 1


if __name__ == '__main__':

    dbObject=Diff_DBUtils()
    dbObject.dbConnect("island1")
    A=NonSortedGA(1,1,1,10,3,100,1, dbObject)

    #A=list()
    #A=NonSortedGA("Tradesheets","PriceSeries",6,'20120622','20121109','20121112','20130111',0.0,2,12)
    #print "Evolved individuals",A
    #NonSortedGA(Tradesheets,PriceSeries,MaxIndividualsInGen,TrainingBegin,TrainingEnd,ReportingBegin, ReportingEnd, CostOfTrading,MaxGen,MaxIndividuals)
    #print "total_Gain,total_DD,total_Profit_Long,total_Loss_Long,total_Win_Long_Trades,total_Loss_Long_Trades,total_Profit_Short,total_Loss_Short,total_Win_Short_Trades,total_Loss_Short_Trades"
