__author__ = 'Monali'

import numpy as np
arr=[]
arr.append(1)
arr.append(5)
arr.append(9)
array= np.random.permutation(arr)
print(array,"uo",array[0])
from DB_DBUtils import *

main_dbObject=DBUtils()
main_dbObject.dbConnect()

from DBUtils_Databases import *
dbObject =Diff_DBUtils()
dbObject.dbConnect("island1")

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


'''
resultIndividuals=main_dbObject.getAllIndividuals()
print "Done"
for i,dum in resultIndividuals:
    print i

'''