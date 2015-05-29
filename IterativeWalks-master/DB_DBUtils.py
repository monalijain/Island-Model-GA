__author__ = 'IIT'

from DatabaseManager import *
from sqlalchemy import create_engine
from decimal import Decimal
import GlobalVariables as gv

class DBUtils:

    databaseObject = None

    def dbConnect (self):
        db_username = gv.db_username
        db_password = gv.db_password
        db_host = gv.db_host
        db_name = gv.name_database
        db_port = gv.db_port
        global databaseObject
        databaseObject = DatabaseManager(db_username, db_password,db_host,db_port, db_name)
        databaseObject.Connect()

    def dbQuery (self, query):
        global databaseObject
        return databaseObject.Execute(query)

    def dbClose (self):
        global databaseObject
        databaseObject.Close()

    # Function to get all individuals from original tradesheet
    def getAllIndividuals(self):
        global databaseObject
        queryIndividuals = "SELECT DISTINCT(individual_id), 1 FROM old_tradesheet_data_table"
        return databaseObject.Execute(queryIndividuals)

    # Function to get trades that are active in a given time interval
    def getTrades (self, startDate, startTime, endDate, endTime):
        global databaseObject
        queryTrades = "SELECT trade_id, individual_id, trade_type, entry_date, entry_time, entry_price, entry_qty, exit_date, exit_time " \
                      "FROM tradesheet_data_table WHERE entry_time<='" + str(endTime) + "' AND exit_time>='" + str(startTime) + \
                      "' AND entry_date='" + str(startDate) + "'"
        return databaseObject.Execute(queryTrades)

    # Function to get new trades from original tradesheet
    def getTradesOrdered (self, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT * FROM old_tradesheet_data_table WHERE entry_date='" + str(date) + "' AND entry_time<'" + str(endTime) + \
                      "' AND entry_time>='" + str(startTime) + "' ORDER BY entry_time"
        #print(queryTrades)
        return databaseObject.Execute(queryTrades)

    def getTradesIndividual(self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryTrades = "SELECT * FROM tradesheet_data_table WHERE entry_date='" + str(startDate) + "' AND entry_time<='" + str(endTime) + \
                      "' AND exit_time>='" + str(startTime) + "' AND individual_id=" + str(individualId)
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit in a given interval
    def getTradesExit(self, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT individual_id, trade_type, entry_qty, entry_price, exit_price FROM tradesheet_data_table WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "' AND exit_time<'" + str(endTime) + "'"
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit at day end
    def getTradesExitEnd(self, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT individual_id, trade_type, entry_qty, entry_price, exit_price FROM tradesheet_data_table WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "'"
        return databaseObject.Execute(queryTrades)

    # Function to get price series in a time range
    # Not being used currently
    def getPriceSeries (self, startDate, startTime, endDate, endTime):
        global databaseObject
        queryPriceSeries = "SELECT time, price FROM price_series_table WHERE date='" + str(startDate) + "' AND time>='" + str(startTime) + \
                           "' AND time<='" + str(endTime) + "'"
        return databaseObject.Execute(queryPriceSeries)

    # Function to get price from price series for a given date and time
    def getPrice(self, startDate, startTime):
        global databaseObject
        queryPrice = "SELECT time, price FROM price_series_table WHERE date='" + str(startDate) + "' AND time='" + str(startTime) + "'"
        return databaseObject.Execute(queryPrice)

    # Function to insert MTM value in db
    def insertMTM(self, individualId, tradeId, tradeType, entryDate, mtmTime, mtm):
        global databaseObject
        queryCheckRecord = "SELECT EXISTS (SELECT 1 FROM mtm_table WHERE trade_id=" + str(tradeId) + " AND date='" + str(entryDate) + \
                           "' AND time='" + str(mtmTime) + "'), 0"

        resultRecord = databaseObject.Execute(queryCheckRecord)
        for result, dummy in resultRecord:
            if result==0:
                queryInsertMTM = "INSERT INTO mtm_table " \
                                 "(trade_id, individual_id, trade_type, date, time, mtm) " \
                                 "VALUES " \
                                 "(" + str(tradeId) + ", " + str(individualId) + ", " + str(tradeType) + \
                                 ", '" + str(entryDate) + "', '" + str(mtmTime) + "', " + str(mtm) + ")"
                return databaseObject.Execute(queryInsertMTM)

    # Function to get last reallocation date for an individual
    # Not being used currently
    def getStartDate (self, individualId):
        queryDate = "SELECT MAX(last_reallocation_date), individual_id FROM reallocation_table WHERE individual_id=" + str(individualId)
        global databaseObject
        return databaseObject.Execute(queryDate)

    # Function to get last reallocation time for an individual
    # Not being used currently
    def getStartTime (self, individualId):
        queryTime = "SELECT MAX(last_reallocation_time), individual_id FROM reallocation_table" \
                    " WHERE individual_id=" + str(individualId) + " AND last_reallocation_date=" \
                    "(SELECT MAX(last_reallocation_date) FROM reallocation_table WHERE individual_id=" + str(individualId) + ")"
        global databaseObject
        return databaseObject.Execute(queryTime)

    # Function to get last reallocation time overall
    # Not being used currently
    def getLastReallocationTime(self):
        queryTime = "SELECT MAX(last_reallocation_time), individual_id FROM reallocation_table" \
                    " WHERE last_reallocation_date=(SELECT MAX(last_reallocation_date) FROM reallocation_table)"
        global databaseObject
        return databaseObject.Execute(queryTime)

    # Function to get last reallocation date overall
    # Not being used currently
    def getLastReallocationDate(self):
        queryDate = "SELECT MAX(last_reallocation_date), individual_id FROM reallocation_table"
        global databaseObject
        return databaseObject.Execute(queryDate)

    # Not being used currently
    def updateStartTime(self, individualId, startDate, startTime):
        global databaseObject
        queryUpdate = "UPDATE reallocation_table SET last_reallocation_date='" + str(startDate) + \
                      "', last_reallocation_time=" + str(startTime) + " WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryUpdate)

    # Function to get net MTM for all long trades
    def getTotalPosMTM (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryMTM = "SELECT SUM(mtm), 1 FROM mtm_table WHERE individual_id=" + str(individualId) +\
                   " AND time>'" + str(startTime) + "' AND date>='" + str(startDate) + \
                   "' AND date<='" + str(endDate) + "' AND time<='" + str(endTime) + \
                   "' AND trade_type=0"
        #print(queryMTM)
        return databaseObject.Execute(queryMTM)

    # function to get total quantity for all long trades
    def getTotalPosQty (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryQty = "SELECT SUM(entry_qty), 1 FROM tradesheet_data_table WHERE individual_id=" \
                   + str(individualId) + " AND entry_time<'" + str(endTime) + "' AND exit_time>'" + str(startTime) + \
                   "' AND entry_date='" + str(startDate) + "' AND trade_type=0"
        #print(queryQty)
        return databaseObject.Execute(queryQty)

    # Function to get net MTM for all short trades
    def getTotalNegMTM (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryMTM = "SELECT SUM(mtm), 1 FROM mtm_table WHERE individual_id=" + str(individualId) + \
                   " AND time>'" + str(startTime) + "' AND date>='" + str(startDate) + \
                   "' AND date<='" + str(endDate) + "' AND time<='" + str(endTime) + \
                   "' AND trade_type=1"
        #print(queryMTM)
        return databaseObject.Execute(queryMTM)

    # Function to get total quantity for all short trades
    def getTotalNegQty (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryQty = "SELECT SUM(entry_qty), 1 FROM tradesheet_data_table WHERE individual_id=" \
                   + str(individualId) + " AND entry_time<'" + str(endTime) + "' AND exit_time>'" + str(startTime) + \
                   "' AND entry_date='" + str(startDate) + "' AND trade_type=1"
        #print(queryQty)
        return databaseObject.Execute(queryQty)

    # Function to get Q Matrix of an individual
    def getQMatrix (self, individualId):
        global databaseObject
        queryQM = "SELECT row_num, column_num, q_value FROM q_matrix_table WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryQM)



    # Function to check if an individual's entry exists in asset_allocation_table
    def checkIndividualAssetExists (self, individualId):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM asset_allocation_table WHERE individual_id=" + str(individualId) + "), 0"
        return databaseObject.Execute(queryCheck)

    # Function to update individual's asset
    def updateIndividualAsset(self, individualId, toBeUsedAsset):
        global databaseObject
        queryOldAsset = "SELECT total_asset, used_asset, free_asset FROM asset_allocation_table WHERE individual_id=" + str(individualId)
        resultOldAsset = databaseObject.Execute(queryOldAsset)
        for totalAsset, usedAsset, freeAsset in resultOldAsset:
            newUsedAsset = float(usedAsset) + toBeUsedAsset
            newFreeAsset = float(freeAsset) - toBeUsedAsset
            queryUpdate = "UPDATE asset_allocation_table SET used_asset=" + str(round(newUsedAsset,4)) + ", free_asset=" + str(round(newFreeAsset,4)) + \
                          " WHERE individual_id=" + str(individualId)
            return databaseObject.Execute(queryUpdate)

    # Function to get the asset being used by an individual at a given time
    # Not used currently
    def getUsedAsset (self, individualId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryUsedAsset = "SELECT entry_qty*entry_price, 1 FROM tradesheet_data_table WHERE individual_id=" + str(individualId) + \
                         " AND entry_date='" + str(startDate) + "' AND entry_time<='" + str(endTime) + "' AND exit_time>'" + str(endTime) + "'"
        return databaseObject.Execute(queryUsedAsset)

    # Function to add individual's entry in reallocation table
    def addNewState(self, individualId, date, time, state):
        global databaseObject
        queryNewState = "INSERT INTO reallocation_table" \
                        " (individual_id, last_reallocation_date, last_reallocation_time, last_state)" \
                        " VALUES" \
                        " (" + str(individualId) + ", '" + str(date) + "', '" + str(time) + "', " + str(state) + ")"
        return databaseObject.Execute(queryNewState)

    # Function to get last state for an individual
    def getLastState (self, individualId):
        global databaseObject
        queryLastState = "SELECT last_state, individual_id FROM reallocation_table WHERE individual_id=" + str(individualId) + \
                         " AND last_reallocation_date=(SELECT MAX(last_reallocation_date) FROM reallocation_table WHERE " \
                         "individual_id=" + str(individualId) + ") AND last_reallocation_time=(SELECT MAX(last_reallocation_time) " \
                        "FROM reallocation_table WHERE individual_id=" + str(individualId) + " AND last_reallocation_date=" \
                        "(SELECT MAX(last_reallocation_date) FROM reallocation_table WHERE individual_id=" + str(individualId) + "))"
        return databaseObject.Execute(queryLastState)


    # Function to reduce free asset for an individual
    def reduceFreeAsset(self, individualId, unitQty):
        global databaseObject
        resultCurrentFreeAsset = databaseObject.Execute("SELECT free_asset, total_asset FROM asset_allocation_table "
                                                        "WHERE individual_id="+str(individualId))
        for freeAsset, totalAsset in resultCurrentFreeAsset:
            if (float(freeAsset)>=unitQty):
                newFreeAsset = float(freeAsset) - unitQty
                newTotalAsset = float(totalAsset) - unitQty
                queryUpdate = "UPDATE asset_allocation_table SET free_asset=" + str(round(newFreeAsset,4)) + \
                              ", total_asset=" + str(round(newTotalAsset,4)) + " WHERE individual_id=" + str(individualId)
                return databaseObject.Execute(queryUpdate)
            else:
                newTotalAsset = float(totalAsset - freeAsset)
                queryUpdate = "UPDATE asset_allocation_table SET free_asset=0, total_asset=" + str(round(newTotalAsset,4)) + \
                              " WHERE individual_id=" + str(individualId)
                return databaseObject.Execute(queryUpdate)


    # Function to get current free asset for an individual
    def getFreeAsset(self, individualId):
        global databaseObject
        queryCheck = "SELECT free_asset, 1 FROM asset_allocation_table WHERE individual_id=" + str(individualId)
        return databaseObject.Execute(queryCheck)


    # Function to return Net Profit-Loss of Long trades within an interval
    def getLongNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty),1 FROM tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=0"
        return databaseObject.Execute(queryPL)

    # Function to return Net Profit-Loss of Short trades within an interval
    def getShortNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty),1 FROM tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=1"
        return databaseObject.Execute(queryPL)

    # Function to return number of Long trades in an interval
    def getLongTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=0"
        return databaseObject.Execute(queryTrades)

    # Function to return number of Short trades in an interval
    def getShortTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=1"
        return databaseObject.Execute(queryTrades)

    # Function to return Net Profit-Loss of Long trades in original table within an interval
    def getRefLongNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty),1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=0"
        return databaseObject.Execute(queryPL)

    # Function to return Net Profit-Loss of Short trades in original table within an interval
    def getRefShortNetPL(self, startDate, endDate):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty),1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + \
                  "' AND entry_date<='" + str(endDate) + "' AND trade_type=1"
        return databaseObject.Execute(queryPL)

    # Function to return number of Long trades in original table within an interval
    def getRefLongTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=0"
        return databaseObject.Execute(queryTrades)

    # Function to return number of Short trades in original table within an interval
    def getRefShortTrades(self, startDate, endDate):
        global databaseObject
        queryTrades = "SELECT COUNT(*),1 FROM old_tradesheet_data_table WHERE entry_date>='" + str(startDate) + "' AND entry_date<='" + str(endDate) + \
                      "' AND trade_type=1"
        return databaseObject.Execute(queryTrades)

    # Function to return asset at month end
    def getAssetMonthly(self, month, year):
        global databaseObject
        queryAsset = "SELECT total_asset, 1 FROM asset_daily_allocation_table WHERE " \
                     "date=(SELECT MAX(date) FROM asset_daily_allocation_table WHERE MONTH(date)=" + str(month) + " AND YEAR(date)=" + str(year) + ")"
        return databaseObject.Execute(queryAsset)

    # Function to return maximum and minimum asset in the month
    def getAssetMonthlyMaxMin(self, month, year):
        global databaseObject
        queryAsset = "SELECT MAX(total_asset), MIN(total_asset) FROM asset_daily_allocation_table WHERE MONTH(date)=" + str(month) + " AND YEAR(date)=" + str(year)
        return databaseObject.Execute(queryAsset)

    # Function to return trades per month
    def getTradesMonthly(self):
        global databaseObject
        queryTrades = "SELECT count(*), MONTH(entry_date), YEAR(entry_date) FROM tradesheet_data_table GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryTrades)

    # Function to return trades per month in base tradesheet
    def getRefTradesMonthly(self):
        global databaseObject
        queryTrades = "SELECT count(*), MONTH(entry_date), YEAR(entry_date) FROM old_tradesheet_data_table GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryTrades)

    # Function to return Long NetPL and Long trades per month
    def getNetPLLongMonthly(self):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM tradesheet_data_table WHERE trade_type=0 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)

    # Function to return Short NetPL and Short trades per month
    def getNetPLShortMonthly(self):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM tradesheet_data_table WHERE trade_type=1 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)

    # Function to return Long NetPL and Long trades per month in base tradesheet
    def getRefNetPLLongMonthly(self):
        global databaseObject
        queryPL = "SELECT SUM((exit_price-entry_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM old_tradesheet_data_table WHERE trade_type=0 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)

    # Function to return Short NetPL and Short trades per month in base tradesheet
    def getRefNetPLShortMonthly(self):
        global databaseObject
        queryPL = "SELECT SUM((entry_price-exit_price)*entry_qty), COUNT(*), MONTH(entry_date), YEAR(entry_date) FROM old_tradesheet_data_table WHERE trade_type=1 GROUP BY YEAR(entry_date), MONTH(entry_date)"
        return databaseObject.Execute(queryPL)
