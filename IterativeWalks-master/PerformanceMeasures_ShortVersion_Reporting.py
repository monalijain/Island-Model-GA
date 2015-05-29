import GlobalVariables as gv
import csv
from DB_DBUtils import *

def CalculatePerformanceMeasuresReporting(IndividualID,walkforward_number,stock_number,dbObject3):

    stringName= "performance_measures_"+"Reporting"+"_walk" + str(walkforward_number)+ "_" + "stock"+ str(gv.stock_number)
    resultPerformanceMeasures= dbObject3.dbQuery("SELECT *, 1 FROM " + stringName)
    Performance_Measures=[]
    for individual_id, netpl_trades, netpl_drawdown, total_drawup, total_drawdown, netpl, total_trades, profit_epochs, k in resultPerformanceMeasures:
            Performance_Measures.append((netpl_trades, netpl_drawdown,total_drawup, total_drawdown, netpl, total_trades, profit_epochs))
            return Performance_Measures

    Performance_Measures.append((-50000,-50000, 0, -50000, -50000, 0, 0.0))
    return Performance_Measures

#p=CalculatePerformanceMeasures("PriceSeries","Tradesheets", 0.0 ,13,'20120402','20120502')
#print p
