__author__ = 'MJ'

import GlobalVariables as gv
import csv
from DBUtils_Databases import *

class PerformanceMeasures:
    def CalculatePerformanceMeasures(self,IndividualID,walkforward_number,stock_number,dbObject2):

        stringName= "performance_measures_"+"Training"+"_walk" + str(walkforward_number)+ "_" + "stock"+ str(gv.stock_number)

        resultPerformanceMeasures= dbObject2.dbQuery("SELECT *, 1 FROM " + stringName+" WHERE individual_id = (SELECT individual_id FROM island_id_table WHERE dummy_id = "+str(IndividualID)+")")
        Performance_Measures=[]
        for individual_id, netpl_trades, netpl_drawdown, total_drawup, total_drawdown, netpl, total_trades, profit_epochs, k in resultPerformanceMeasures:
            Performance_Measures.append((netpl_trades, netpl_drawdown,total_drawup, total_drawdown, netpl, total_trades, profit_epochs))
            return Performance_Measures
        Performance_Measures.append((-50000,-50000, 0, -50000, -50000, 0, 0.0))
        return Performance_Measures


if __name__ == '__main__':
    dbObject2=Diff_DBUtils()
    dbObject2.dbConnect("island1")
    perfm=PerformanceMeasures()
    P=perfm.CalculatePerformanceMeasures(3,1,1,dbObject2)
    print(P)
