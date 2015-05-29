__author__ = 'Ciddhi'

from DBUtils import *

if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()

    dbObject.dbQuery("CREATE TABLE latest_individual_table"
                     " ("
                     " individual_id int"
                     " )")

    dbObject.dbQuery("CREATE TABLE training_mtm_table"
                     " ("
                     " trade_id int,"
                     " individual_id int,"
                     " trade_type int,"
                     " date date,"
                     " time time,"
                     " mtm float"
                     " )")

    dbObject.dbQuery("CREATE TABLE training_tradesheet_data_table"
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

    dbObject.dbQuery("CREATE TABLE training_asset_allocation_table"
                     " ("
                     " individual_id int,"
                     " total_asset decimal(15,4),"
                     " used_asset decimal(15,4),"
                     " free_asset decimal(15,4)"
                     " )")

    dbObject.dbQuery("CREATE TABLE ranking_table"
                     " ("
                     " individual_id int,"
                     " ranking int"
                     " )")

    dbObject.dbQuery("CREATE TABLE old_tradesheet_data_table"
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

    dbObject.dbQuery("CREATE TABLE asset_daily_allocation_table"
                     "("
                     "date date,"
                     "time time,"
                     "total_asset decimal(15,4)"
                     ")")

    dbObject.dbQuery("CREATE TABLE tradesheet_data_table"
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


    '''
    dbObject.dbQuery("CREATE TABLE price_series_table"
                     " ("
                     " date date,"
                     " time time,"
                     " price float"
                     " )")
    '''
    dbObject.dbQuery("CREATE TABLE mtm_table"
                     " ("
                     " trade_id int,"
                     " individual_id int,"
                     " trade_type int,"
                     " date date,"
                     " time time,"
                     " mtm float"
                     " )")

    dbObject.dbQuery("CREATE TABLE asset_allocation_table"
                     " ("
                     " individual_id int,"
                     " total_asset decimal(15,4),"
                     " used_asset decimal(15,4),"
                     " free_asset decimal(15,4)"
                     " )")

    dbObject.dbQuery("CREATE TABLE q_matrix_table"
                     " ("
                     " individual_id int,"
                     " row_num int,"
                     " column_num int,"
                     " q_value decimal(20,10)"
                     " )")

    dbObject.dbQuery("CREATE TABLE reallocation_table"
                     " ("
                     " individual_id int,"
                     " last_reallocation_date date,"
                     " last_reallocation_time time,"
                     " last_state int"
                     " )")
    '''
    query1 = "LOAD DATA INFILE 'E:/Studies/MTP/MTP Code/IndividualInfo.csv'" \
             " INTO TABLE old_tradesheet_data_table" \
             " FIELDS TERMINATED BY ','" \
             " ENCLOSED BY '\"'" \
             " LINES TERMINATED BY '\\r\\n'"
    print(query1)
    dbObject.dbQuery(query1)

    query2 = "LOAD DATA INFILE 'E:/Studies/MTP/MTP Code/SamplePriceSeriesNew.csv'" \
             " INTO TABLE price_series_table" \
             " FIELDS TERMINATED BY ','" \
             " LINES TERMINATED BY '\\r\\n'"

    print(query2)
    dbObject.dbQuery(query2)

    '''
    dbObject.dbClose()

