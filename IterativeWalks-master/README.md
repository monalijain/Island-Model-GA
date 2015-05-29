# Parallel_GA
This is the version of GA works on databases (but is serialized) (parallelize it later)

1. You need to enter the desired values of the variables, in GlobalVariables.py file.
2. For each stock, store the PriceSeries.csv file, and Tradesheet Table, in a folder, in data folder of mariaDB.
3. Also change the 
        db_username
        db_password 
        db_host 
        db_port 
    in DB_DBUtils.py as needed.
4. Note: You need to run parallel_GA for each stock (store stock number (int) in global variables), in different pycharm windows.
5. Run the Wrapper_Parallel file for each stock.

IMPORTANT: The TradesheetTables columns are named as: TradeID, IndividualID, EntryDate, etc... and not Trade_ID, Individual_ID, etc..