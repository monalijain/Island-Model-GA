__author__ = 'Monali'
import IslandParameters as ip
import random
from DBUtils_Databases import *
def Calculate_Migrating_Islands():
    numIslands=ip.NumSubpop
    fromIsland= [[0 for x in range(numIslands)] for x in range(numIslands)]
    toIsland= [[0 for x in range(numIslands)] for x in range(numIslands)]

    for num1 in range(0,numIslands):
        for num2 in range(0,numIslands):
            if(num1==num2):
                continue
            fromIsland[num1][num2]=num1+1
            toIsland[num1][num2]=num2+1

    selectedToIsland=[]
    selectedFromIsland=[]

    done=0
    while(done==0):
        selectedToIsland=[]
        selectedFromIsland=[]
        counter=0
        for num1 in range(0,numIslands):
            k=random.choice(toIsland[num1])
            if(len(selectedToIsland)!=0):
                counter=0
                while(not (k!=0 and k not in selectedToIsland)):
                    k=random.choice(toIsland[num1])
                    #print "NO"
                    counter += 1
                    if(counter>1000):
                        break


            else:
                while(k==0):
                    k=random.choice(toIsland[num1])

            if(counter>1000):
                break
            else:
                selectedToIsland.append(k)
                selectedFromIsland.append(num1+1)
                print "Migration From ",num1+1," To ",k

        if(counter<=1000):
            done=1

        print selectedToIsland
        print selectedFromIsland

    return [selectedFromIsland, selectedToIsland]

def Migrate(walkFwd):
    [selectedFromIsland, selectedToIsland]=Calculate_Migrating_Islands()
    string_1="performance_measures_training_walk"+str(walkFwd)+"_stock"+str(gv.stock_number)

    for num in range(0,len(selectedFromIsland)):
        FromIsland="island"+str(selectedFromIsland[num])
        ToIsland="island"+str(selectedToIsland[num])
        dbObjectFrom=Diff_DBUtils()
        dbObjectFrom.dbConnect(FromIsland)

        dbObjectTo=Diff_DBUtils()
        dbObjectTo.dbConnect(ToIsland)

        dbObjectFrom.dbQuery("INSERT INTO "+ToIsland+".island_population_table"+" (SELECT * FROM island_migration_pop_table ORDER BY trade_id)")
        dbObjectTo.dbQuery("ALTER TABLE island_population_table ORDER BY trade_id")

        dbObjectFrom.dbQuery("INSERT INTO "+ToIsland+"."+string_1+" SELECT * FROM island_migration_performance_measures")
        dbObjectTo.dbQuery("ALTER TABLE "+string_1+" ORDER BY individual_id")

        dbObjectFrom.dbQuery("INSERT INTO "+ToIsland+".island_id_table (individual_id, island_id)"+" SELECT individual_id, "+str(selectedToIsland[num])+" FROM island_migration_id_table")
        dbObjectTo.dbQuery("ALTER TABLE island_id_table DROP COLUMN dummy_id")
        dbObjectTo.dbQuery("ALTER TABLE island_id_table ORDER BY individual_id")
        dbObjectTo.dbQuery("ALTER TABLE island_id_table ADD dummy_id INT UNSIGNED NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (dummy_id)")

        dbObjectFrom.dbClose()
        dbObjectTo.dbClose()

#Migrate(1)