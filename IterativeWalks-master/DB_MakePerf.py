import datetime
import csv
import GlobalVariables as gv
from DB_DBUtils import *
import logging
def Make_Performance_Measures(Begin,End,Train_Or_Report,walkforward_number):
    dbObject1 = DBUtils()
    dbObject1.dbConnect()
    #Begin=gv.TBeginList[gv.num]
    #End=gv.TendList[gv.num]
    BeginDate=datetime.datetime.strptime(Begin,"%Y%m%d")
    EndDate=datetime.datetime.strptime(End,"%Y%m%d")

    #countDates=dbObject2.dbQuery("SELECT COUNT(DISTINCT(date)),1 FROM price_series_table WHERE date >= '" + str(BeginDate.date())+ "' AND date <= '"+str(EndDate.date())+"'")
    resultDates = dbObject1.dbQuery("SELECT DISTINCT(date),1 FROM "+ gv.priceSeriesTable +" WHERE date >= '" + str(BeginDate.date())+ "' AND date <= '"+str(EndDate.date())+"'")
    #dbObject2.dbClose()

    DictOfDates={}
    date_count_range=0
    for date,count in resultDates:
        CurrentDate=datetime.datetime.strptime(str(date),"%Y-%m-%d")
        DictOfDates[CurrentDate.date()]=date_count_range
        date_count_range=date_count_range+1
        #print(DictOfDates[CurrentDate.date()])

    #print DictOfDates

    #print BeginDate.date()
    #print EndDate.date()

    query= "SELECT *, 1 FROM " + gv.name_Tradesheet_Table + " WHERE entrydate >= '"+str(BeginDate.date()) + "' AND entrydate <= '"+str(EndDate.date())+"'"
    logging.info("Executing Query %s",query)
    resultIndividuals = dbObject1.dbQuery(query)

    c=0
    #Parameters for each individual
    Performance_Measures=[]                    #List of Tuples to store the Performance_Measures the calculation_type
    #Lists to store the variables for daily calculations
    tmp_daily_running_pl=[]
    #Variables for the total period of between begin date and end date
    total_Win_Long_Trades=0
    total_Win_Short_Trades=0
    total_Loss_Long_Trades=0
    total_Loss_Short_Trades=0
    total_Profit_Long=0
    total_Profit_Short=0
    total_Loss_Long=0
    total_Loss_Short=0

    #Creating a list which stores daily PL values
    for filler_date_count in range(date_count_range):
        tmp_daily_running_pl.append(0)

    date_count=0
    diff=0
    DD_History = []          #List to store the DD values.
    Gain_History = []        #List to store the MaxGain values

    string_1= "performance_measures_"+Train_Or_Report+"_walk" + str(walkforward_number)+ "_" + "stock"+ str(gv.stock_number)

    IndividualIDExist=0
    for trade_id, individual_id, trade_type, trade_entry_date, trade_entry_time, trade_entry_price, trade_qty, trade_exit_date, trade_exit_time, trade_exit_price, k in resultIndividuals:
        IndividualIDExist=1
        CurrentDate=datetime.datetime.strptime(str(trade_entry_date),"%Y-%m-%d")
        #print CurrentDate, individual_id
# CurrentDate=datetime.datetime.strptime(str(trade_entry_date),"%Y-%m-%d")
# dateinrange=CurrentDate.date()<=EndDate.date() and CurrentDate.date()>=BeginDate.date()

        if(c==0):
            logging.info("Start of calculation of performance measures:")
            prev_trade_entry_date=trade_entry_date
           # CurrentDate= datetime.datetime.strptime(str(prev_trade_entry_date),"%Y-%m-%d")
            #print CurrentDate, individual_id
            #print individual_id, "get in the loop"
        elif(prev_individual_id==individual_id):
            #print individual_id,"get in the loop"
            diff=1
            #CurrentDate= datetime.datetime.strptime(str(prev_trade_entry_date),"%Y-%m-%d")
            #print CurrentDate, individual_id
        else:
    #calculation of DD:
            #print tmp_daily_running_pl
            #CurrentDate= datetime.datetime.strptime(str(prev_trade_entry_date),"%Y-%m-%d")
            #print CurrentDate, individual_id
            if(tmp_daily_running_pl[0] < 0):
                DD_History.append(tmp_daily_running_pl[0])
            else:
                DD_History.append(0)
            #print DD_History,'0'
            #print date_count_range
            DD_date_count = 1
            while(DD_date_count < date_count_range):
                if(tmp_daily_running_pl[DD_date_count]<0):
                    DD_History.append(DD_History[DD_date_count-1]+tmp_daily_running_pl[DD_date_count])
                else:
                    DD_History.append(0)
                DD_date_count = DD_date_count +1
                #print DD_History,DD_date_count-1
            #print prev_individual_id, DD_History
            total_DD=0
            for DD_Daily_Value in DD_History:
                if(DD_Daily_Value<total_DD):
                    total_DD=DD_Daily_Value

            #if the individual (trade) is not feasible  i.e. Total Trades or the drawdown is 0 , that means its not feasible
            TotalTrades= total_Win_Short_Trades+total_Loss_Short_Trades+total_Win_Long_Trades+total_Loss_Long_Trades

            if(TotalTrades==0 or total_DD==0):
                Performance_Measures.append((-50000,-50000, 0, -50000, -50000, 0, 0.0))
            else:
            #calculation of MaxGain (Similar to DD Calculation):
                if(tmp_daily_running_pl[0]>0):
                    Gain_History.append(tmp_daily_running_pl[0])
                else:
                    Gain_History.append(0)

                Gain_date_count=1
                while(Gain_date_count <date_count_range):
                    if(tmp_daily_running_pl[Gain_date_count]>0):
                        Gain_History.append(Gain_History[Gain_date_count-1]+tmp_daily_running_pl[Gain_date_count])
                    else:
                        Gain_History.append(0)
                    Gain_date_count+=1

                total_Gain=0.0
                for Gain_Daily_Value in Gain_History:
                    if(Gain_Daily_Value>total_Gain):
                        total_Gain=Gain_Daily_Value

                NetPL=total_Profit_Long+total_Loss_Long+total_Profit_Short+total_Loss_Short
                ProfitMakingEpochs=1.0*(total_Win_Long_Trades+total_Win_Short_Trades)/(1.0*(TotalTrades))
                Performance_Measures.append((NetPL/(TotalTrades), NetPL/(-total_DD), total_Gain, total_DD, NetPL, TotalTrades, ProfitMakingEpochs))

            #print prev_individual_id,Performance_Measures

            dbObject1.dbQuery("INSERT INTO "+ string_1+""
                                       " (individual_id, netpl_trades, netpl_drawdown, total_drawup, total_drawdown, netpl, total_trades, profit_epochs)"
                                       " VALUES"
                                       " ('" + str(prev_individual_id) + "', '" + str(Performance_Measures[0][0]) + "', '" + str(Performance_Measures[0][1]) + "', '" + str(Performance_Measures[0][2]) + "', '" + str(Performance_Measures[0][3]) + "', '" + str(Performance_Measures[0][4]) + "', '" + str(Performance_Measures[0][5]) + "', " + str(Performance_Measures[0][6]) + ")" )

            Performance_Measures=[]                    #List of Tuples to store the Performance_Measures the calculation_type
            #Lists to store the variables for daily calculations
            tmp_daily_running_pl=[]
            #Variables for the total period of between begin date and end date
            total_Win_Long_Trades=0
            total_Win_Short_Trades=0
            total_Loss_Long_Trades=0
            total_Loss_Short_Trades=0
            total_Profit_Long=0
            total_Profit_Short=0
            total_Loss_Long=0
            total_Loss_Short=0

            #Creating a list which stores daily PL values
            for filler_date_count in range(date_count_range):
                tmp_daily_running_pl.append(0)

            date_count=0
            diff=0
            DD_History = []          #List to store the DD values.
            Gain_History = []        #List to store the MaxGain values


        if(DictOfDates.has_key(CurrentDate.date())):
            date_count= DictOfDates[CurrentDate.date()]
            #print(CurrentDate.date(),DictOfDates[CurrentDate.date()], individual_id)
            #Trade type 0=FALSE is short and 1=TRUE is long
            if(trade_type==1):
                trade_pl = ((trade_exit_price - trade_entry_price)-(trade_exit_price+trade_entry_price)*gv.cost_of_trading)*trade_qty
                if(trade_pl>0):
                    total_Win_Long_Trades+=1
                    total_Profit_Long+=trade_pl
                else:
                    total_Loss_Long_Trades+=1
                    total_Loss_Long+=trade_pl
            else:
                trade_pl = ((trade_entry_price - trade_exit_price)-(trade_exit_price+trade_entry_price)*gv.cost_of_trading)*trade_qty
                if(trade_pl>0):
                    total_Win_Short_Trades+=1
                    total_Profit_Short+=trade_pl
                else:
                    total_Loss_Short_Trades+=1
                    total_Loss_Short+=trade_pl

            tmp_daily_running_pl[date_count] = tmp_daily_running_pl[date_count]+trade_pl
            #print tmp_daily_running_pl,"netpl"
        c=1
        prev_individual_id=individual_id
        prev_trade_entry_date=trade_entry_date

    #    writePerf.writerow([IndividualID,Performance_Measures[0][0],Performance_Measures[0][1],Performance_Measures[0][2],Performance_Measures[0][3],Performance_Measures[0][4],Performance_Measures[0][5],Performance_Measures[0][6]])
#For the last individual
    if(tmp_daily_running_pl[0] < 0):
        DD_History.append(tmp_daily_running_pl[0])
    else:
        DD_History.append(0)

    DD_date_count = 1
    while(DD_date_count < date_count_range):
        if(tmp_daily_running_pl[DD_date_count]<0):
            DD_History.append(DD_History[DD_date_count-1]+tmp_daily_running_pl[DD_date_count])
        else:
            DD_History.append(0)
        DD_date_count = DD_date_count +1

    total_DD=0
    for DD_Daily_Value in DD_History:
        if(DD_Daily_Value<total_DD):
            total_DD=DD_Daily_Value

    #if the individual (trade) is not feasible  i.e. Total Trades or the drawdown is 0 , that means its not feasible
    TotalTrades= total_Win_Short_Trades+total_Loss_Short_Trades+total_Win_Long_Trades+total_Loss_Long_Trades

    if(TotalTrades==0 or total_DD==0):
        Performance_Measures.append((-50000,-50000, 0, -50000, -50000, 0, 0.0))
    else:
        #calculation of MaxGain (Similar to DD Calculation):
        if(tmp_daily_running_pl[0]>0):
            Gain_History.append(tmp_daily_running_pl[0])
        else:
            Gain_History.append(0)

        Gain_date_count=1
        while(Gain_date_count <date_count_range):
            if(tmp_daily_running_pl[Gain_date_count]>0):
                Gain_History.append(Gain_History[Gain_date_count-1]+tmp_daily_running_pl[Gain_date_count])
            else:
                Gain_History.append(0)
            Gain_date_count+=1

        total_Gain=0.0
        for Gain_Daily_Value in Gain_History:
            if(Gain_Daily_Value>total_Gain):
                total_Gain=Gain_Daily_Value

        NetPL=total_Profit_Long+total_Loss_Long+total_Profit_Short+total_Loss_Short
        ProfitMakingEpochs=1.0*(total_Win_Long_Trades+total_Win_Short_Trades)/(1.0*(TotalTrades))
        Performance_Measures.append((NetPL/(TotalTrades), NetPL/(-total_DD), total_Gain, total_DD, NetPL, TotalTrades, ProfitMakingEpochs))

    #print individual_id,Performance_Measures
    if(IndividualIDExist!=0):
        dbObject1.dbQuery("INSERT INTO "+ string_1+""
                                           " (individual_id, netpl_trades, netpl_drawdown, total_drawup, total_drawdown, netpl, total_trades, profit_epochs)"
                                           " VALUES"
                                           " ('" + str(individual_id) + "', '" + str(Performance_Measures[0][0]) + "', '" + str(Performance_Measures[0][1]) + "', '" + str(Performance_Measures[0][2]) + "', '" + str(Performance_Measures[0][3]) + "', '" + str(Performance_Measures[0][4]) + "', '" + str(Performance_Measures[0][5]) + "', " + str(Performance_Measures[0][6]) + ")" )

    logging.info("End of Calculation of Performance Measures")
    dbObject1.dbClose()

#Make_Performance_Measures('20120622','20120628',"Training",1)
