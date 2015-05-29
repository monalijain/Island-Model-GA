__author__ = 'Monali'
from DB_DBUtils import *
import IslandParameters as ip
from DBUtils_Databases import *
from CreateDatabase import CreateDB
import GlobalVariables as gv
import numpy as np
if __name__ == "__main__":
    main_dbObject=DBUtils()
    main_dbObject.dbConnect()
    resultIndividuals = main_dbObject.dbQuery("SELECT DISTINCT(individual_id), 1 FROM "+gv.name_Tradesheet_Table)

    array_ID=[]
    count_individuals=0
    for IndividualID,dummy in resultIndividuals:
        array_ID.append(IndividualID)
        count_individuals +=1
        #print(array_ID)

    print "Number of Individuals in the original tradesheet: ",count_individuals


    perm = np.random.permutation(array_ID)
    Island_Individuals = count_individuals/ip.NumSubpop

    for i in range(1,ip.NumSubpop+1):
        CreatedbObject=CreateDB(gv.db_username,gv.db_password,gv.db_host,gv.db_port)
        CreatedbObject.Connect()
        CreatedbObject.Execute("CREATE DATABASE island"+str(i))
        dbObject =Diff_DBUtils()
        dbObject.dbConnect("island"+str(i))

        query="CREATE TABLE island_generation_table(island_id int, generation int, individual_id int)"
        print(query)
        dbObject.dbQuery(query)


        query="CREATE TABLE island_migration_pop_table(trade_id int, individual_id int, trade_type int, entry_date date, entry_time time, entry_price float, entry_qty int, exit_date date, exit_time time, exit_price float )"
        print (query)
        dbObject.dbQuery(query)

        string_1= "CREATE TABLE island_migration_performance_measures"
        print string_1
        dbObject.dbQuery(" " + string_1 + " "
                             "("
                             "individual_id int,"
                             "netpl_trades float,"
                             "netpl_drawdown float,"
                             "total_drawup float,"
                             "total_drawdown float,"
                             "netpl float,"
                             "total_trades int,"
                             "profit_epochs float"
                             ")")

        query="CREATE TABLE island_pareto_front_table(island_id int, generation int, individual_id int)"
        print(query)
        dbObject.dbQuery(query)

        #query="CREATE TABLE island_population_table"+str(i)+"("
        query="CREATE TABLE island_population_table(trade_id int, individual_id int, trade_type int, entry_date date, entry_time time, entry_price float, entry_qty int, exit_date date, exit_time time, exit_price float )"
        print query
        dbObject.dbQuery(query)

        dbObject.dbQuery("CREATE TABLE price_series_table"
                             "("
                             "date date,"
                             "time time,"
                             "price float"
                             ")")

        main_dbObject.dbQuery("INSERT INTO island"+str(i)+".price_series_table SELECT * FROM "+gv.name_database+"."+gv.priceSeriesTable)


        start=Island_Individuals*(i-1)
        for counter in range(start,i*Island_Individuals):
            main_dbObject.dbQuery("INSERT INTO island"+str(i)+".island_population_table SELECT * FROM "+gv.name_database+"."+gv.name_Tradesheet_Table+" WHERE individual_id = "+str(perm[counter]))



        CreatedbObject.Close()
        dbObject.dbClose()
