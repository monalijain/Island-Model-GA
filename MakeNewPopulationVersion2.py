__author__ = 'MJ'

import random
def MakeNewPopulation(F,Pt1,i,gen,MaxIndividualsInGen,lengthOfFront1And2):
    SumFitnessValues=0
    j=1
    while j<=i:
        if(bool(F[j])):
            for k in range(0,len(F[j])):
                SumFitnessValues=SumFitnessValues+(i-j)
            j=j+1
        else:
            j=j+1
            continue

    Q=list()
    for num in range(0,MaxIndividualsInGen-lengthOfFront1And2):
        rand=random.randint(1, SumFitnessValues)
        sum=0
        j=1
        while (rand>sum and j<=i):
            counter = 0
            if(bool(F[j])):
                while(rand>sum and counter<len(F[j])):
                    sum=sum+(i-j)
                    counter=counter+1
                j=j+1
            else:
                j=j+1
                continue

        while(not(bool(F[j-1]))):
            j=j-1

        StoreKeys=F[j-1].keys()
        StoreIndividualID1 = random.choice(StoreKeys)
        #print "StoreIndividualID1",StoreIndividualID1


        randFront=random.randint(1,i)
        while(not(bool(F[randFront]))):
            randFront=random.randint(1,i)

        StoreKeys=F[randFront].keys()
        #print StoreKeys
        StoreIndividualID2=random.choice(StoreKeys)
        #print "StoreIndividualID2",StoreIndividualID2

        NewIndividualID= Crossover(StoreIndividualID1,StoreIndividualID2,gen,MaxIndividualsInGen)

        if NewIndividualID not in Q:
            Q.append(NewIndividualID)

    return Q

def Crossover(StoreIndividualID1,StoreIndividualID2,gen,MaxIndividualsInGen):
    NewIndividualID=gen*MaxIndividualsInGen+(StoreIndividualID1+StoreIndividualID2)%MaxIndividualsInGen+1
    return NewIndividualID
