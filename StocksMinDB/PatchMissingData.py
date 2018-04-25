import csv
import datetime as dt
import time
import tushare as ts
import os
from gmsdk import md
import numpy as np
import pandas as pd

from StocksMinDB.StocksMinDb import StocksMinDB
from StocksMinDB.Constants import ByDay,ByStock

def utc2local(utctime,format):
    lctm = time.localtime(utctime)
    timestr = time.strftime(format,lctm)
    return int(timestr)

def OneDayMinData_MD(date,stklst):
    md.init('18201141877','Wqxl7309')
    datestr= dt.datetime.strptime(str(date),'%Y%m%d')
    datestr = dt.datetime.strftime(datestr,'%Y-%m-%d')
    starttime = ' '.join([datestr,'09:30:00'])
    endtime = ' '.join([datestr,'15:00:00'])
    # day base obj
    objday = StocksMinDB(configpath=r'E:\stocks_data_min\StocksMinDB\configs')
    objday.connectDB('stocks_data_min_by_day')
    daycols = [c.lower() for c in ByDay.colinfo if c!='STKID']
    dayconn = objday._getConn_()
    # stk base obj
    objstk = StocksMinDB(configpath=r'E:\stocks_data_min\StocksMinDB\configs')
    objstk.connectDB('stocks_data_min_by_stock')
    stkcols = [c.lower() for c in ByStock.colinfo]
    stkconn = objstk._getConn_()
    # data to extract
    cols = ['date','time','stkcd','open','high','low','close','volume','amount']
    alldata = pd.DataFrame()
    for stk in stklst:
        szstk = False
        if stk>=600000:
            stkcd = '.'.join(['SHSE',str(stk)])
        else:
            szstk = True
            stkstr = str(stk)
            stkcd = '.'.join(['SZSE','0'*(6-len(stkstr))+stkstr])
        stkbars = md.get_bars(stkcd,60,starttime,endtime)
        stkdata = [[date,utc2local(bar.utc_endtime,'%H%M'),stk,bar.open,bar.high,bar.low,bar.close,bar.volume,bar.amount] for bar in stkbars]
        if szstk and stkdata[-1][1]==1500 and stkdata[-2][1]>=1457:
            tms = stkdata[-2][1]+1
            cls = stkdata[-2][6]
            for tm in range(tms,1460):
                stkdata.insert(-1,[date,tm,stk,cls,cls,cls,cls,0,0])
        stkdata = pd.DataFrame(stkdata,columns=cols)
        stkdata['volamtflag'] = 0
        stkdata.loc[stkdata['amount']>(stkdata['volume']+100)*stkdata['high'],'volamtflag'] = 1
        stkdata.loc[stkdata['amount']<(stkdata['volume']-100)*stkdata['low'],'volamtflag'] = 2
        stkdata.loc[(stkdata['amount']==0) & (stkdata['volume']>0),'volamtflag'] = 3
        stkdata.loc[(stkdata['amount']==0) & (stkdata['volume']==0),'volamtflag'] = 4
        alldata = alldata.append(stkdata,ignore_index=True)
        stktable = '_'.join(['stkmin',stkcd[:2].lower()+stkcd.split('.')[1]])
        objstk.update_db(conn=stkconn,
                         dbname='stocks_data_min_by_stock',
                         data=stkdata.loc[:,stkcols].values,
                         tablename=stktable,
                         colinfo=ByStock.colinfo,
                         prmkey=ByStock.prmkey,
                         if_exist='append',
                         chunksize=1000
                         )
    # update day base
    daytable = 'stkmin_{0}'.format(date)
    daycolinfo = dict([itm for itm in ByDay.colinfo.items() if itm[0]!='STKID'])
    objday.update_db(conn=dayconn,
                         dbname='stocks_data_min_by_day',
                         data=alldata.loc[:,daycols].values,
                         tablename=daytable,
                         colinfo=daycolinfo,
                         prmkey=ByDay.prmkey,
                         if_exist='append',
                         chunksize=1000
                         )

def takemin(tm):
    tm = tm.split(':')
    hour = int(tm[0])
    min = int(tm[1])
    if min==59:
        min = 0
        hour+=1
    elif not ((min==0 and hour==15) or (min==30 and hour==11)):
        min+=1
    return hour*100+min

def tushare_tick2min(date,stkcd):
    datestr= dt.datetime.strptime(str(date),'%Y%m%d')
    datestr = dt.datetime.strftime(datestr,'%Y-%m-%d')
    stkstr = str(stkcd)
    stkstr = stkstr if len(stkstr)==6 else '0'*(6-len(stkstr))+stkstr
    tickdata = ts.get_tick_data(stkstr,date=datestr)
    if tickdata.empty or ('没有数据' in tickdata.iloc[0][0]):
        return None
    tickdata = tickdata.loc[:,['time','price','volume','amount']]
    tickdata['time'] = tickdata['time'].map(takemin)
    tickdata = tickdata.sort_values(by=['time'])
    minbars = pd.DataFrame()
    minrow = np.zeros([7,])
    cols = ['time','open','high','low','close','volume','amount']
    obsnum = tickdata.shape[0]
    for row in range(obsnum):
        tickrow = tickdata.iloc[row].values
        if tickrow[0]!=minrow[0]:
            if row>0:
                minbars = minbars.append(pd.DataFrame([minrow],columns=cols),ignore_index=True)
            minrow[0] = tickrow[0] # time
            minrow[1] = tickrow[1] # open
            minrow[2] = tickrow[1] # high
            minrow[3] = tickrow[1] # low
            minrow[4] = tickrow[1] # close
            minrow[5] = tickrow[2]*100 # volume
            minrow[6] = tickrow[3] # amount
        else:
            if tickrow[1]>minrow[2]: minrow[2] = tickrow[1] # high
            if tickrow[1]<minrow[3]: minrow[3] = tickrow[1] # low
            minrow[4] = tickrow[1] # close
            minrow[5] += tickrow[2]*100 # volume
            minrow[6] += tickrow[3] # amount
    minbars = minbars.append(pd.DataFrame([minrow],columns=cols),ignore_index=True)
    minbars['stkcd'] = stkcd
    minbars['date'] = date
    minbars['volamtflag'] = 0
    minbars.loc[minbars['amount']>(minbars['volume']+100)*minbars['high'],'volamtflag'] = 1
    minbars.loc[minbars['amount']<(minbars['volume']-100)*minbars['low'],'volamtflag'] = 2
    minbars.loc[(minbars['amount']==0) & (minbars['volume']>0),'volamtflag'] = 3
    minbars.loc[(minbars['amount']==0) & (minbars['volume']==0),'volamtflag'] = 4
    minbars = minbars[minbars['time']>=931]
    return minbars


def OneDayMinData_TS(date,stklst):
    # day base obj
    objday = StocksMinDB(configpath=r'E:\stocks_data_min\StocksMinDB\configs')
    objday.connectDB('stocks_data_min_by_day')
    daycols = [c.lower() for c in ByDay.colinfo if c!='STKID']
    dayconn = objday._getConn_()
    # stk base obj
    objstk = StocksMinDB(configpath=r'E:\stocks_data_min\StocksMinDB\configs')
    objstk.connectDB('stocks_data_min_by_stock')
    stkcols = [c.lower() for c in ByStock.colinfo]
    stkconn = objstk._getConn_()
    # all data of list
    with open('.\patched\{0}.txt'.format(date),'a+') as file:
        ms = open('.\missed\{0}.txt'.format(date),'a+')
        # alldata = pd.DataFrame()
        stillmiss = [date]
        for stkcd in stklst:
            stkdata = tushare_tick2min(stkcd=stkcd,date=date)
            if stkdata is None:
                stillmiss.append(stkcd)
                print('No data for {0} on date {1}'.format(stkcd,date))
                ms.writelines('{0}'.format(stkcd)+'\n')
                continue
            # alldata = alldata.append(stkdata,ignore_index=True)
            # updating stk base
            stkstr = str(stkcd)
            stkexch = 'sh'+stkstr if stkcd>=600000 else 'sz'+'0'*(6-len(stkstr))+stkstr
            stktable = '_'.join(['stkmin',stkexch])
            objstk.update_db(conn=stkconn,
                             dbname='stocks_data_min_by_stock',
                             data=stkdata.loc[:,stkcols].values,
                             tablename=stktable,
                             colinfo=ByStock.colinfo,
                             prmkey=ByStock.prmkey,
                             if_exist='append',
                             chunksize=1000
                             )
            # updating day base
            daytable = 'stkmin_{0}'.format(date)
            daycolinfo = dict([itm for itm in ByDay.colinfo.items() if itm[0]!='STKID'])
            objday.update_db(conn=dayconn,
                                 dbname='stocks_data_min_by_day',
                                 data=stkdata.loc[:,daycols].values,
                                 tablename=daytable,
                                 colinfo=daycolinfo,
                                 prmkey=ByDay.prmkey,
                                 if_exist='append',
                                 chunksize=1000
                             )
            file.writelines('{0}'.format(stkcd)+'\n')
        ms.close()



def update(wait=10*60):
    try:
        datelst = os.listdir(r'.\patched')
        datelst = sorted([int(c.split('.')[0]) for c in datelst])
        with open(r'E:\stocks_data_min\StocksMinDB\still_lost_stklst.csv') as f:
            for line in f.readlines():
                ln = line.strip().split(',')
                date = int(ln[0].strip())
                stklst = [int(v) for v in ln[1:] if v]
                lastdate = 20050101 #datelst[-1]
                if date<lastdate:
                    if os.path.exists(r'E:\stocks_data_min\StocksMinDB\patched\{0}.txt'.format(date)):
                        with open(r'E:\stocks_data_min\StocksMinDB\patched\{0}.txt'.format(date)) as df:
                            finished = [int(stk.strip()) for stk in df.readlines()]
                    else:
                        finished = []
                    if os.path.exists(r'E:\stocks_data_min\StocksMinDB\missed\{0}.txt'.format(date)):
                        with open(r'E:\stocks_data_min\StocksMinDB\missed\{0}.txt'.format(date)) as mf:
                            missed = [int(stk.strip()) for stk in mf.readlines()]
                    else:
                        missed = []
                    nofinished = list(set(stklst) - set(finished)-set(missed))
                    print(date,nofinished)
                    if not nofinished:
                        continue
                    else:
                        # OneDayMinData_MD(date,stklst)
                        OneDayMinData_TS(date,nofinished)
    except OSError as e:
        print(e)
        print('connection failed, waiting {0} seconds'.format(wait))
        for tm in range(wait,0,-1):
            print(tm)
            time.sleep(1)
        update(wait)


if __name__=='__main__':
    update()
