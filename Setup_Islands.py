__author__ = 'Monali'
from DB_DBUtils import *
import IslandParameters as ip
from DBUtils_Databases import *
from CreateDatabase import CreateDB
import GlobalVariables as gv
import numpy as np


def Setup_Islands(walkforward_number):

    main_dbObject=DBUtils()
    main_dbObject.dbConnect()
    main_dbObject.dbQuery("CREATE TABLE id_table(individual_id int, dummy int)")
    main_dbObject.dbQuery("INSERT INTO id_table (individual_id, dummy) SELECT DISTINCT(IndividualID), 0 FROM "+gv.name_Tradesheet_Table+" ORDER BY RAND()")

    result_individuals=main_dbObject.dbQuery("SELECT COUNT(individual_id), 1 FROM id_table")

    for i,dummy in result_individuals:
        count_individuals=i

    print "Number of Individuals in the original tradesheet: ",count_individuals

    Island_Individuals = count_individuals/ip.NumSubpop
    print(Island_Individuals)

    for i in range(1,ip.NumSubpop+1):
        print "ISLAND ",i
        CreatedbObject=CreateDB(gv.db_username,gv.db_password,gv.db_host,gv.db_port)
        CreatedbObject.Connect()
        CreatedbObject.Execute("DROP DATABASE IF EXISTS island"+str(i))
        #CreatedbObject.Execute("DROP DATABASE IF EXISTS island"+str(i))
        CreatedbObject.Execute("CREATE DATABASE island"+str(i))
        dbObject =Diff_DBUtils()
        dbObject.dbConnect("island"+str(i))

        query="CREATE TABLE island_generation_table(generation int, individual_id int)"
        print(query)
        dbObject.dbQuery(query)

        query="CREATE TABLE island_migration_pop_table(trade_id int, individual_id int, trade_type int, entry_date date, entry_time time, entry_price float, entry_qty int, exit_date date, exit_time time, exit_price float )"
        print (query)
        dbObject.dbQuery(query)

        query="CREATE TABLE island_migration_id_table(individual_id int, island_id int)"
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

        query="CREATE TABLE island_pareto_front_table(generation int, individual_id int)"
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

        query="INSERT INTO island"+str(i)+".price_series_table SELECT * FROM "+gv.name_database+"."+gv.priceSeriesTable
        print query
        main_dbObject.dbQuery(query)

        query="CREATE TABLE island_id_table(individual_id int, island_id int)"
        print query
        dbObject.dbQuery(query)

        query="INSERT INTO island"+str(i)+".island_id_table (individual_id, island_id) (SELECT individual_id, "+str(i)+" FROM id_table LIMIT "+str(Island_Individuals)+" OFFSET "+str((i-1)*Island_Individuals)+")"
        print query
        main_dbObject.dbQuery(query)

        dbObject.dbQuery("ALTER TABLE island_id_table ORDER BY individual_id")
        dbObject.dbQuery("ALTER TABLE island_id_table ADD dummy_id INT UNSIGNED NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (dummy_id)")


        query="INSERT INTO island"+str(i)+".island_population_table (SELECT "+gv.name_Tradesheet_Table+".* FROM "+gv.name_Tradesheet_Table+" INNER JOIN island"+str(i)+".island_id_table ON "+gv.name_Tradesheet_Table+".IndividualID  = island"+str(i)+".island_id_table.individual_id ORDER BY TradeID)"
        print query
        main_dbObject.dbQuery(query)



        string_1= "CREATE TABLE performance_measures_"+"Training"+"_walk" + str(walkforward_number)+ "_" + "stock"+ str(gv.stock_number)
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


        CreatedbObject.Close()
        dbObject.dbClose()

    return Island_Individuals

#islnn=Setup_Islands(1)
