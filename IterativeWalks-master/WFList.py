__author__ = 'MJ'
#Function which gives walkforward list, given the price series data; Number of Days in Training Data; Number of Days in Reporting Date
import GlobalVariables as gv
from DB_DBUtils import *
import logging
import datetime

def CreateWFList(SamplePriceSeries,T,R):
    dbObject4 = DBUtils()
    dbObject4.dbConnect()
    query= "SELECT *, 1 FROM " + gv.priceSeriesTable
    logging.info("Executing Query %s",query)
    resultDates = dbObject4.dbQuery(query)

    c=0
    Tcounter=0
    TrainingBegin=list()
    TrainingEnd=list()

    for date,time,price,count in resultDates:
        date=datetime.datetime.strptime(str(date), '%Y-%m-%d').strftime('%Y%m%d')
        if(c==0):
            TrainingBegin.append(str(date))
            Tcounter+=1
            #print row[0]
        else:
            if(date1!=date):
                Tcounter+=1
                #print row[0]
                if(Tcounter%(R)==1):
                    TrainingBegin.append(str(date))
                if(Tcounter==T):
                    TrainingEnd.append(str(date))
                if(Tcounter>T and (Tcounter-T)%R==0):
                    TrainingEnd.append(str(date))
        c=c+1
        date1=date

    query= "SELECT *, 1 FROM " + gv.priceSeriesTable
    logging.info("Executing Query %s",query)
    resultDates = dbObject4.dbQuery(query)

    Rcounter=1
    ReportingBegin=list()
    ReportingEnd=list()
    c=0
    Rcount=0
    for date,time,price,count in resultDates:
        date=datetime.datetime.strptime(str(date), '%Y-%m-%d').strftime('%Y%m%d')
        if(c!=0):
            if(date1!=date):
                try:
                    i = TrainingEnd.index(str(date1))
                except ValueError:
                    i = -1 # no match

                if(i!=-1):
                    ReportingBegin.append(str(date))
                    Rcount=1
                    Rcounter=0

                if(Rcount==1):
                    Rcounter+=1
                if(Rcounter==R):
                    ReportingEnd.append(str(date))
                    Rcount=0
                    Rcounter=0

        c=c+1
        date1=date

    while(len(TrainingBegin)!= len(ReportingEnd)):
        TrainingBegin.pop()

    while(len(TrainingEnd)!= len(ReportingEnd)):
        TrainingEnd.pop()

    while(len(ReportingEnd)!= len(ReportingBegin)):
        ReportingBegin.pop()

    #return (['20120622','20120628'],['20120625','20120629'],['20120626','20120702'],['20120627','20120703'])
    dbObject4.dbClose()
    
    return (TrainingBegin,TrainingEnd,ReportingBegin,ReportingEnd)

#[a,b,c,d]=CreateWFList("PriceSeries", 20, 10)
