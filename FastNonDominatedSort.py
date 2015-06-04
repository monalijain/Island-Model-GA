__author__ = 'MJ'

import operator

def FastNonDominatedSort(Rt):
    F={}
    FrontOfPopulation={}
    PopulationStore=Rt.items()
    length=len(PopulationStore)

    num=0
    othernum=1
    #Sorting on the basis of first objective function
    PopulationStore.sort(key=lambda x: x[1][num])

    if(PopulationStore[length-1][1][num]!=PopulationStore[length-2][1][num]):
        FrontOfPopulation[PopulationStore[length-1][0]]=1
        #print FrontOfPopulation
        adjacentFront_1=1
        t=length-2
    else:
        t=length-1
        adjacentFront_1=0

    while(t>0):
        if(PopulationStore[t][1][num]==PopulationStore[t-1][1][num]):
            store={}
            store[t]=[PopulationStore[t][0],PopulationStore[t][1][othernum]]
            store[t-1]=[PopulationStore[t-1][0],PopulationStore[t-1][1][othernum]]
            #print store
            t=t-1
            while(t>0 and PopulationStore[t][1][num]==PopulationStore[t-1][1][num]):
                store[t-1]=[PopulationStore[t-1][0],PopulationStore[t-1][1][othernum]]
                t=t-1

            #print store
            storeItems=store.items()
            storeItems.sort(key=lambda x:x[1][1])
            storeLength=len(storeItems)
            adjacentVal_2=storeItems[storeLength-1][1][1]

            #print storeItems
            FrontOfPopulation[storeItems[storeLength-1][1][0]]=adjacentFront_1+1
            adjacentFront_2=FrontOfPopulation[storeItems[storeLength-1][1][0]]

            l=storeLength-2
            for l in range(storeLength-2,-1,-1):
                if(storeItems[l][1][1]==adjacentVal_2):
                    FrontOfPopulation[storeItems[l][1][0]]=adjacentFront_2
                else:
                    FrontOfPopulation[storeItems[l][1][0]]=adjacentFront_2+1

                adjacentFront_2=FrontOfPopulation[storeItems[l][1][0]]
                adjacentVal_2=storeItems[l][1][1]

            adjacentFront_1=adjacentFront_2
            adjacentVal_1=PopulationStore[t][1][num]

        else:
            FrontOfPopulation[PopulationStore[t][0]]=adjacentFront_1+1
            adjacentFront_1=adjacentFront_1+1
            adjacentVal_1=PopulationStore[t][1][num]

        t=t-1

    if(t==0):
        FrontOfPopulation[PopulationStore[t][0]]=adjacentFront_1+1
        adjacentFront_1=adjacentFront_1+1

 #   print FrontOfPopulation

#Sorting on the basis of second objective function
    num=1
    othernum=0
    #Sorting on the basis of first objective function
    PopulationStore.sort(key=lambda x: x[1][num])

    if(PopulationStore[length-1][1][num]!=PopulationStore[length-2][1][num]):
        FrontOfPopulation[PopulationStore[length-1][0]]=1
        #print FrontOfPopulation
        adjacentFront_1=1
        t=length-2
    else:
        t=length-1
        adjacentFront_1=0

    while(t>0):
        if(PopulationStore[t][1][num]==PopulationStore[t-1][1][num]):
            store={}
            store[t]=[PopulationStore[t][0],PopulationStore[t][1][othernum]]
            store[t-1]=[PopulationStore[t-1][0],PopulationStore[t-1][1][othernum]]
            #print store
            t=t-1
            while(t>0 and PopulationStore[t][1][num]==PopulationStore[t-1][1][num]):
                store[t-1]=[PopulationStore[t-1][0],PopulationStore[t-1][1][othernum]]
                t=t-1

            #print store
            storeItems=store.items()
            storeItems.sort(key=lambda x:x[1][1])
            storeLength=len(storeItems)
            adjacentVal_2=storeItems[storeLength-1][1][1]

            #print storeItems
            FrontOfPopulation[storeItems[storeLength-1][1][0]]=min(adjacentFront_1+1,FrontOfPopulation[storeItems[storeLength-1][1][0]])
            adjacentFront_2=adjacentFront_1+1
            l=storeLength-2
            for l in range(storeLength-2,-1,-1):
                if(storeItems[l][1][1]==adjacentVal_2):
                    FrontOfPopulation[storeItems[l][1][0]]=min(adjacentFront_2,FrontOfPopulation[storeItems[l][1][0]])

                else:
                    FrontOfPopulation[storeItems[l][1][0]]=min(adjacentFront_2+1,FrontOfPopulation[storeItems[l][1][0]])
                    adjacentFront_2=adjacentFront_2+1

                adjacentVal_2=storeItems[l][1][1]

            adjacentFront_1=adjacentFront_2
            adjacentVal_1=PopulationStore[t][1][num]

        else:
            FrontOfPopulation[PopulationStore[t][0]]=min(FrontOfPopulation[PopulationStore[t][0]],adjacentFront_1+1)
            adjacentFront_1=adjacentFront_1+1
            adjacentVal_1=PopulationStore[t][1][num]

        t=t-1

    if(t==0):
        FrontOfPopulation[PopulationStore[t][0]]=min(FrontOfPopulation[PopulationStore[t][0]],adjacentFront_1+1)
        adjacentFront_1=adjacentFront_1+1

#    print FrontOfPopulation
    counter= max(FrontOfPopulation.iteritems(), key=operator.itemgetter(1))[1]

    for i in range(1,1+counter):
       F[i]={}

    for p in FrontOfPopulation:
        #print p
        F[FrontOfPopulation[p]][p]=Rt[p]

    return F

'''
Pt={}
Pt[0]=[1,1,1,1,1,1,1]
Pt[4]=[3,3,2,2,2,2,2]
Pt[3]=[4,4,0,0,0,0,0]
F=FastNonDominatedSort(Pt)

numFronts=max(F.keys())
print "Num of fronts",numFronts

individualList = []
rankList = []
rank=1
for j in range(1,numFronts+1):
    for individual in F[j].keys():
        individualList.append(individual)
        rankList.append(rank)
        rank=rank+1
print individualList


F=FastNonDominatedSort(Pt)
numFronts=max(F.keys())
rank=1
for j in range(1,numFronts+1):
    for individual in F[j].keys():
        print (individual,rank)
        #dbObject.updateRank(individual, rank)
        rank=rank+1

'''