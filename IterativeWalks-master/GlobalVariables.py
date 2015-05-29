__author__ = 'Monali'

'''
from DB_DBUtils import *
dbObject3=DBUtils()
dbObject3.dbConnect()

'''

#Enter these values
stock_number=1 #Stock number
cost_of_trading=0.0 #Cost of trading
numDaysInTraining=20 #Number of days in training period
numDaysInReporting=10 # Number of days in testing period
name_database='raw'#'individualinfo_ver3' # Name of the database where, PriceSeries.csv and name_Tradesheet_table is stored
db_username = 'root'
db_password = 'mj031992'#'controljp'
db_host = '127.0.0.1'
db_port = '3306'
priceSeriesTable="price_series_table" #Name of the price series table in the database

#The columns in tradsheet table are:
#TradeID, IndividualID, TradeType, EntryDate, EntryTime, EntryPrice, EntryQty, ExitDate, ExitTime, ExitPrice
name_Tradesheet_Table="one"#"old_tradesheet_data_table" #name of the tradesheet table


MaxIndividualsInGen=10000 #Maximum Individuals that you want in each generation

#Dont change these for now
MinimumGen=2 #Minimum Generations for which the program should run, provided those many individuals exist
CheckGen=3 #Number of generations for which the convergence will be checked
ConvergenceValue=0.01 #Value of NetPL/Total Trades



'''


#from WFList import CreateWFList
#[TBeginList,TendList,RBeginList,REndList]=CreateWFList("PriceSeries", 20, 10) #Creates Walkforward List
#[TBeginList,TendList,RBeginList,REndList]=[["20120622"],["20120628"],["20121112"],["20130111"]]
'''
