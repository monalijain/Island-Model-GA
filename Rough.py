__author__ = 'Monali'

from DB_DBUtils import *
main_dbObject=DBUtils()
main_dbObject.dbConnect()
import GlobalVariables as gv

main_dbObject.dbQuery("CREATE TABLE b_table (indiv_id_dummy int, individual_id int NOT NULL AUTO_INCREMENT, PRIMARY KEY(individual_id))")
#main_dbObject.dbQuery("INSERT INTO o_table (id, b) SELECT DISTINCT(individual_id), 1 FROM "+gv.name_Tradesheet_Table+" LIMIT 5")
#main_dbObject.dbQuery("INSERT INTO b_table (indiv_id_dummy) VALUES (1,2,3,4,5)")

#main_dbObject.dbQuery("UPDATE b_table INNER JOIN o_table on o_table.b = b_table.individual_id SET b_table.indiv_id_dummy = o_table.id")

#main_dbObject.dbQuery("UPDATE b_table SET b_table.indiv_id_dummy = o_table.id")

'''
import mysql.connector.pooling

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

from DatabaseManager import *
cursor = cnx1.cursor()
cursor.execute("SELECT * FROM island_id_table")
for indiv in cursor:
    print indiv



from DBUtils_Databases import *
from NonSortedGANewVersion import *
dbObject=Diff_DBUtils()
dbObject.dbConnect("island2")
Ob=NonSortedGANewVersion()
[A,D,StoreParetoID]=Ob.NonSortedGA(initial_individualID=0,generation=1,walkforward_number=1,MaxIndividualsInGen=gv.MaxIndividualsInGen,MaxGen=5,MaxIndividuals=100,dbObject=dbObject,islandID=2)


import IslandParameters as ip


def createMigratingPopulation(threadName, threadID, populationSize, Epochs, dbObject,walkFwd):

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

from DBUtils_Databases import *
dbObject=Diff_DBUtils()
i=5
dbObject.dbConnect("island"+str(i))

query="CREATE TABLE island_migration_id_table(individual_id int, island_id int)"
dbObject.dbQuery(query)
dbObject.dbQuery("CREATE TABLE island_ID_table(individual_id int, island_id int)")
dbObject.dbQuery("INSERT INTO island_ID_table (individual_id, island_id) SELECT DISTINCT(individual_id), "+str(i)+" FROM "+ip.name_Tradesheet_Table)

createMigratingPopulation("Thread"+str(i),i,100,1,dbObject,1)

arr=[]
arr.append(1)
arr.append(5)
arr.append(9)
array= np.random.permutation(arr)
print(array,"uo",array[0])
array.sort()
print array, "sorted"
from DB_DBUtils import *

main_dbObject=DBUtils()
main_dbObject.dbConnect()

from DBUtils_Databases import *
dbObject =Diff_DBUtils()
dbObject.dbConnect("island2")

import random
numIslands=ip.NumSubpop
fromIsland= [[0 for x in range(numIslands)] for x in range(numIslands)]
toIsland= [[0 for x in range(numIslands)] for x in range(numIslands)]

for num1 in range(0,numIslands):
    for num2 in range(0,numIslands):
        if(num1==num2):
            continue
        fromIsland[num1][num2]=num1+1
        toIsland[num1][num2]=num2+1


done=0
while(done==0):
    selectedToIsland=[]
    selectedFromIsland=[]
    counter=0
    for num1 in range(0,numIslands):
        k=random.choice(toIsland[num1])
        if(len(selectedToIsland)!=0):
            counter=0
            while(not (k!=0 and k not in selectedToIsland)):
                k=random.choice(toIsland[num1])
                #print "NO"
                counter += 1
                if(counter>1000):
                    break


        else:
            while(k==0):
                k=random.choice(toIsland[num1])

        if(counter>1000):
            break
        else:
            selectedToIsland.append(k)
            selectedFromIsland.append(num1+1)
            print "Migration From ",num1+1," To ",k

    if(counter<=1000):
        done=1

    print selectedToIsland
    print selectedFromIsland

walkID=1
from DBUtils_Databases import *
dbObject =Diff_DBUtils()
dbObject.dbConnect("island1")

query="CREATE TABLE island_migration_id_table(individual_id int, island_id int)"
dbObject.dbQuery(query)

string_1="performance_measures_training_walk"+str(walkID)+"_stock"+str(gv.stock_number)
dbObject.dbQuery("INSERT INTO island_migration_performance_measures (SELECT "+string_1+".* FROM "+string_1+" INNER JOIN island_migration_id_table ON "+string_1+".individual_id  = island_migration_id_table.individual_id ORDER BY island_migration_id_table.individual_id)")
dbObject.dbQuery("DELETE "+string_1+".* FROM "+string_1+" INNER JOIN island_migration_id_table ON "+string_1+".individual_id  = island_migration_id_table.individual_id")


dbObject.dbQuery("CREATE TABLE island_population_table"
                     " ("
                     " trade_id int,"
                     " individual_id int,"
                     " trade_type int,"
                     " entry_date date,"
                     " entry_time time,"
                     " entry_price float,"
                     " entry_qty int,"
                     " exit_date date,"
                     " exit_time time,"
                     " exit_price float"
                     " )")

main_dbObject.dbQuery("INSERT INTO island1.island_population_table SELECT * FROM individualinfo_ver3.old_tradesheet_data_table WHERE individual_id = "+str(array[0]))



resultIndividuals=main_dbObject.getAllIndividuals()
print "Done"
for i,dum in resultIndividuals:
    print i



dbObject.dbQuery("CREATE TABLE island_ID_table(individual_id int, island_id int)")
dbObject.dbQuery("INSERT INTO island_ID_table (individual_id, island_id) SELECT DISTINCT(individual_id), "+str(2)+" FROM "+ip.name_Tradesheet_Table)
'''