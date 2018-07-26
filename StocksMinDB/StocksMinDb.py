
import configparser as cp
import datetime as dt
import logging
import multiprocessing as mpr
import os
import time
import sys

import csv
import h5py
import scipy.io as scio
import mysql.connector
import numpy as np
import pandas as pd

from StocksMinDB.Constants import LogMark,TableCol,ByDay,ByStock,DB_NOTES


class StocksMinDB:

    def __init__(self,configpath,corenum=1):
        cfp = cp.ConfigParser()
        cfp.read(os.path.join(configpath,'loginfo.ini'))
        self._loginfo = dict(cfp.items('login'))
        cfp.read(os.path.join(configpath,'datainfo.ini'))
        self._updtpath = cfp.get('datasource','update')
        self._histpath = cfp.get('datasource','history')
        self._tmpupdt = cfp.get('other','updtfld')
        ######### cpu核数量 ###### 如果大于1 则采用POOL
        self._corenum = corenum #min(corenum,10)
        assert self._corenum>0
        self.conn = None

    def _logger(self,logname):
        ######## create logger # 按实际调用日期写日志  ########
        logfile = os.path.join('logs','{0}_{1}.log'.format(logname,dt.datetime.today().strftime('%Y%m%d')))
        if not os.path.exists(logfile):
            os.system('type NUL > {0}'.format(logfile))
        logger = logging.getLogger(name=__name__)
        logger.setLevel(level=logging.DEBUG)
        fh = logging.FileHandler(logfile, mode='a') # 输出到file
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(LogMark.formatter)
        ch = logging.StreamHandler() # 输出到屏幕
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(LogMark.formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)
        return logger

    def _currDB_(self,conn):
        cursor = conn.cursor()
        cursor.execute('SELECT DATABASE()')
        currdb = cursor.fetchone()[0]
        return currdb

    def _getConn_(self,seed=None):
        if seed is None:
            return self.conn[0]
        else:
            assert seed<self._corenum
            return self.conn[seed]

    def _switchDB_(self,seed,dbname):
        if self.conn is None:
            self.connectDB(dbname=dbname)
            return
        conn = self._getConn_(seed=seed)
        if dbname!=self._currDB_(conn=conn):
            conn.cursor().execute('USE {0}'.format(dbname))
            print('seed {0} : connected to {1}'.format(seed,dbname))

    def connectDB(self,dbname):
        if self.conn is None:
            try:
                self.conn = []
                for dumi in range(self._corenum):
                    conn = mysql.connector.connect(**self._loginfo)
                    conn.cursor().execute('USE {0};'.format(dbname))
                    self.conn.append(conn)
                    print('seed {0} : connected to {1}'.format(dumi,dbname))
                print('{0}connected to database {1} with {2}-connection pool'.format(LogMark.info,dbname,self._corenum))
            except mysql.connector.Error as e:
                print('{0}connect fails : {1}'.format(LogMark.error,str(e)))
        else:
            for dumi in range(self._corenum):
                if self._currDB_(conn=self.conn[dumi])==dbname:
                    print('seed {0} : already connected to {1}'.format(dumi,dbname))
                else:
                    self.conn[dumi].cursor.execute('USE {0}'.format(dbname))
                    print('seed {0} : connected to {1}'.format(dumi,dbname))

    def _get_db_tables_all(self,dbname,seed=None):
        """获取指定数据库的所有表格"""
        self._switchDB_(seed=seed,dbname=dbname)
        conn = self._getConn_(seed=seed)
        if dbname != self._currDB_(conn=conn):
            self._switchDB_(seed=seed,dbname=dbname)
        cursor = conn.cursor()
        cursor.execute('SHOW TABLES;')
        temptbs = cursor.fetchall()
        return [tb[0] for tb in temptbs if tb[0]!='trddates'] if temptbs else temptbs

    def _get_db_tables_split(self,dbname,seed):
        alltbs = self._get_db_tables_all(dbname=dbname,seed=seed)
        if seed is None:
            assert self._corenum==1
            return alltbs
        else:
            return [val for ct,val in enumerate(alltbs,1) if ct%self._corenum==seed]

    def _get_filelst(self,filepath,seed=None):
        """ 分派文件 专为并发更新by_stk数据库使用 """
        total_lst = os.listdir(filepath)
        if seed is None:
            assert self._corenum==1
            return total_lst
        else:
            return [val for ct,val in enumerate(total_lst,1) if ct%self._corenum==seed]

    def _get_trddates(self,conn,dbname='stocks_data_min_by_day'):
        cursor = conn.cursor()
        cursor.execute('USE {}'.format(dbname))
        cursor.execute('SELECT * FROM trddates')
        return [dt[0] for dt in cursor.fetchall()]

    def update_db(self,conn,dbname,data,tablename,colinfo,prmkey=None,if_exist='nothing',chunksize=1000):
        """ 将 单张表格 数据更新至 指定数据库
            data : np.array of size obsnum*colnum
            colinfo : 列名：列类型的dict
        """
        cursor = conn.cursor()
        if self._currDB_(conn=conn)!=dbname:
            cursor.execute('USE {0}'.format(dbname))
        # data info
        obsnum = data.shape[0]
        colnames = list(colinfo.keys())
        colnum = len(colnames)
        # saved tables
        cursor.execute('SHOW TABLES;')
        temptbs = cursor.fetchall()
        savedtables = [tb[0] for tb in temptbs if tb[0]!='trddates'] if temptbs else temptbs
        hastable = tablename in savedtables
        if hastable and (if_exist=='nothing'): # 数据表已存在且不会替换
            print('{0}table {1} already in database {2}'.format(LogMark.info,tablename,dbname))
            insertdata = False
        elif (hastable and if_exist=='replace') or (not hastable): # 需要创建新表格
            if hastable: # 需要先删除原表格
                cursor.execute('DROP TABLE {0}'.format(tablename))
                print('{0}table {1} dropped from database {2}'.format(LogMark.info,tablename,dbname))
            ############# 创建表格 #############
            colstr = '('+','.join(['{0} {1}'.format(cn,colinfo[cn]) for cn in colinfo])
            prmkey = ',PRIMARY KEY (' + ','.join(prmkey) + '))' if prmkey else ')'
            egn = 'ENGINE=InnoDB DEFAULT CHARSET=utf8'
            createline = ' '.join(['CREATE TABLE {0} '.format(tablename),colstr,prmkey,egn])
            try:
                cursor.execute(createline)
                print('{0}create table {1} successfully in database {2}'.format(LogMark.info,tablename,dbname))
            except mysql.connector.Error as e:
                print('{0}create table {1} failed in database {2},err : {3}'.format(LogMark.error,tablename,dbname,str(e)))
                raise e
            insertdata = True
        elif hastable and if_exist=='append':
            insertdata = True
        else:
            raise BaseException('if_exist value {0} error'.format(if_exist))
        ############# 插入表格 #############
        if insertdata:
            insertline  = 'INSERT INTO {0} ('.format(tablename) + ','.join(colnames) + ') VALUES '
            try:
                st = time.time()
                chunknum = int(np.ceil(obsnum/chunksize))
                for ck in range(chunknum):
                    head = ck*chunksize
                    tail = min((ck+1)*chunksize,obsnum)
                    chunkdata = data[head:tail,:]
                    toinsert = ','.join([''.join(['(',','.join(['{'+'{0}'.format(i)+'}' for i in range(colnum)]),')']).format(*rowdata) for rowdata in chunkdata])
                    exeline = ''.join([insertline,toinsert])
                    cursor.execute(exeline)
            except BaseException as e:
                if (hastable and if_exist=='replace') or (not hastable): # 需要创建新表格的情况下
                    print('[*]droping table {0}'.format(tablename))
                    cursor.execute('DROP TABLE {0}'.format(tablename))  # 如果更新失败需要确保表格删除
                    print('{0}table {1} dropped from database {2}'.format(LogMark.info,tablename,dbname))
                print('{0}update table {1} failed in database {2}, line No.{3} ,err : {4}'.format(LogMark.error,tablename,dbname,ck,str(e)))
                raise e
            else:
                conn.commit()
                print('{0}table {1} updated successfully in database {2} with {3} lines and {4} seconds'.format(LogMark.info,tablename,dbname,obsnum,time.time()-st))

    ####################################################################################################################
    ################################# 补充 return 列 ###################################
    ####################################################################################################################
    def _get_mat(self,theDate=None,theStock=None):
        # read SINGLE date or stock
        trdDates = scio.loadmat(r'E:\bqfcts\bqfcts\data\trddates.mat')['trddates'][:,0]
        stkCodes = scio.loadmat(r'E:\bqfcts\bqfcts\data\stkinfo.mat')['stkinfo'][:,0]
        if (theDate is None) and (theStock is None):
            raise BaseException('At least one date or stock should be supported')
        if (theDate is not None) and (theStock is not None):
            raise BaseException('Should take only date OR stock')
        takeDate = theStock is None
        ##### read the data #######
        firstDate = np.argwhere(trdDates==DB_NOTES.DB_FIRST_DATE)[0][0]
        firstCurr = 6000
        histStkNum = 3433
        palDate = 0
        palClose = 4
        palPctChg = 7
        if takeDate:
            datePos = np.argwhere(trdDates==theDate)
            if not datePos.shape[0]:
                raise BaseException('date {} is NOT a valid trade date in trdDates.mat'.format(theDate))
            else:
                datePos = datePos[0][0]
            if datePos<firstCurr:
                matName = r'data_19901219_20170630.mat'
                stkcds = stkCodes[:histStkNum]
            else:
                matName = r'data_20150701_now.mat'
                datePos = datePos - firstCurr
                stkcds = stkCodes[:]
            matPath = r'E:\bqfcts\bqfcts\data\Pal\{}'.format(matName)
            pal = h5py.File(matPath)['Pal']
            data = pd.DataFrame(np.column_stack([np.transpose(pal[[palDate,palClose,palPctChg],datePos,:]),stkcds]),columns=['date','close','pctchg','stkcd'])
        else:
            stkPos = np.argwhere(stkCodes==theStock)
            if not stkPos.shape[0]:
                raise BaseException('stock code {} is NOT a valid code'.format(theStock))
            else:
                stkPos = stkPos[0][0]
            currPal = h5py.File(r'E:\bqfcts\bqfcts\data\Pal\data_20150701_now.mat')['Pal']
            if stkPos<histStkNum:
                histPal = h5py.File(r'E:\bqfcts\bqfcts\data\Pal\data_19901219_20170630.mat')['Pal']
                data = pd.DataFrame(np.transpose(np.column_stack([histPal[[palDate,palClose,palPctChg],firstDate:firstCurr,stkPos],
                                                                  currPal[[palDate,palClose,palPctChg],:,stkPos]])),
                                    columns=['date','close','pctchg'])
            else:
                data = pd.DataFrame(np.transpose(currPal[[palDate,palClose,palPctChg],:,stkPos]),columns=['date','close','pctchg'])
        return data[data['date']>0]

    def _patch_rets_by_day(self,seed=None,trdDates=None):
        dbname='stocks_data_min_by_day'
        self._switchDB_(seed=seed,dbname=dbname)
        conn = self._getConn_(seed=seed)
        cursor = conn.cursor()
        trdDates = self._get_trddates(conn=conn,dbname=dbname) if trdDates is None else trdDates
        preColInfo = {k:ByDay.colinfo[k] for k in ByDay.colinfo.keys() if k!=TableCol.stkid}
        # selfupdt = 20171201
        # trdDates = [dt for dt in trdDates if dt>=selfupdt]
        with open('updated_dates.txt') as fl:
            donelist = [d.strip() for d in fl.readlines()]
        for trdt in trdDates:
            start = time.time()
            if (trdt in DB_NOTES.DB_MISSING_DATE) or (trdt<DB_NOTES.DB_FIRST_DATE) or (trdt in donelist):
                continue
            predata = pd.read_sql('SELECT * FROM stkmin_{}'.format(trdt),con=conn)
            predata[TableCol.ret] = predata.groupby([TableCol.stkcd])[TableCol.close].diff()/predata[TableCol.close].shift()
            pal = self._get_mat(theDate=int(trdt)).set_index('stkcd')
            stkHeadIdx = predata[TableCol.stkcd].diff()!=0  # 须确保是按照 股票代码、时间 排序的
            stkHead = predata[stkHeadIdx].set_index(TableCol.stkcd)
            coIndex = list(set(pal.index) & set(stkHead.index))
            # 计算第一分钟收益
            min1Ret = (1+pal.loc[coIndex,'pctchg'])/(pal.loc[coIndex,'close']/stkHead.loc[coIndex,TableCol.close]) - 1
            stkHead.loc[coIndex,TableCol.ret] = min1Ret.loc[coIndex]
            # 补回第一分钟收益
            predata.loc[np.argwhere(stkHeadIdx.values)[:,0],TableCol.ret] = stkHead[TableCol.ret].values
            predata.loc[np.argwhere(np.isinf(predata[TableCol.ret]).values | np.isnan(predata[TableCol.ret]).values)[:,0],TableCol.ret] = 0
            colinfo = preColInfo
            self.update_db(conn=conn,data=predata.values,tablename='stkmin_{}'.format(trdt),colinfo=colinfo,prmkey=ByDay.prmkey,dbname=dbname,if_exist='replace')
            with open('updated_dates.txt','a+') as fl:
                fl.writelines('{}\n'.format(trdt))
            print('stkmin_{0} updated with {1} seconds'.format(trdt,time.time()-start))

    def _patch_rets_by_stock(self,seed=None,allTables=None):
        dbname='stocks_data_min_by_stock'
        self._switchDB_(seed=seed,dbname=dbname)
        conn = self._getConn_(seed=seed)
        backupConn = mysql.connector.connect(**self._loginfo)
        backupCursor = backupConn.cursor()
        backupCursor.execute('USE backup_by_stock')
        allTables = self._get_db_tables_all(dbname=dbname) if allTables is None else allTables
        with open('updated_stocks.txt') as fl:
            donelist = [d.strip() for d in fl.readlines()]
        for tb in allTables:
            start = time.time()
            if int(tb[-6:]) in DB_NOTES.DB_MISSING_STOCK or int(tb[-6:]) in DB_NOTES.MAT_MISSING_STOCK or tb in donelist:
                continue
            ## backup table first
            # backupCursor.execute('CREATE TABLE {0} LIKE {1}.{2}'.format(tb,dbname,tb))
            # backupCursor.execute('INSERT {0} SELECT * FROM {1}.{2}'.format(tb,dbname,tb))
            # backupConn.commit()
            # print('{0} backuped in database {1}'.format(tb,'backup_by_stock'))
            # ##
            predata = pd.read_sql('SELECT * FROM {}'.format(tb),con=conn)
            predata[TableCol.ret] = predata.groupby([TableCol.date])[TableCol.close].diff()/predata[TableCol.close].shift()
            stkHeadIdx = predata[TableCol.date].diff()!=0  # 须确保是按照 日期、时间 排序的
            stkHead = predata[stkHeadIdx].set_index(TableCol.date)
            pal = self._get_mat(theStock=int(tb[-6:])).set_index('date')
            coIndex = list(set(pal.index) & set(stkHead.index))
            #   计算第一分钟
            min1Ret = (1+pal.loc[coIndex,'pctchg'])/(pal.loc[coIndex,'close']/stkHead.loc[coIndex,TableCol.close]) - 1
            stkHead.loc[coIndex, TableCol.ret] = min1Ret.loc[coIndex]
            #   补回第一分钟
            predata.loc[np.argwhere(stkHeadIdx.values)[:, 0], TableCol.ret] = stkHead[TableCol.ret].values
            predata.loc[np.argwhere(np.isinf(predata[TableCol.ret]).values | np.isnan(predata[TableCol.ret]).values)[:,0],TableCol.ret] = 0
            self.update_db(conn=conn,data=predata.values,tablename=tb,colinfo=ByStock.colinfo,prmkey=ByStock.prmkey,dbname=dbname,if_exist='replace')
            with open('updated_stocks.txt','a+') as fl:
                fl.writelines(tb+'\n')
            print('{0} updated with {1} seconds'.format(tb,time.time()-start))

    ####################################################################################################################
    ##############################  缺失数据检查   ####################################################
    ####################################################################################################################
    def _lost_stks(self):
        dbname='stocks_data_min_by_day'
        self.connectDB(dbname=dbname)
        alltables = self._get_db_tables_all(dbname=dbname)
        conn = self._getConn_()
        cursor = conn.cursor()
        trddates = self._get_trddates(conn)
        datelst = [dt for dt in trddates if dt<=20170831 and dt>=19990726]
        start = time.time()
        daily = scio.loadmat(r'stkmark.mat')['stkmark']
        daystk = daily[:,0]
        with open('lost_stklst.csv','w',newline='') as f:
            writer = csv.writer(f)
            for num,dt in enumerate(datelst):
                t1 = time.time()
                if 'stkmin_{0}'.format(dt) not in alltables:
                    print('missing trddate {0}'.format(dt))
                    continue
                exeline = 'SELECT DISTINCT stkcd FROM stkmin_{0} WHERE time IN (1000,1100,1330,1430)'.format(dt)
                cursor.execute(exeline)
                minlst = set([stk[0] for stk in cursor.fetchall()])
                daylst = set(daystk[daily[:,num+2]>0])
                miss = list(daylst-minlst)
                if not miss:
                    print(dt,'no missing')
                    continue
                miss.insert(0,dt)
                writer.writerow(miss)
                print(miss,time.time()-t1)
        print(time.time()-start)

    def _still_lost_stks(self):
        dbname = 'stocks_data_min_by_stock'
        self.connectDB(dbname=dbname)
        conn = self._getConn_()
        cursor = conn.cursor()
        with open('still_lost_stklst.csv','w',newline='') as wf:
            writer = csv.writer(wf)
            with open(r'E:\stocks_data_min\StocksMinDB\lost_stklst.csv') as f:
                for line in f.readlines():
                    ln = line.strip().split(',')
                    date = int(ln[0].strip())
                    stklst = [int(v) for v in ln[1:] if v]
                    miss = [date]
                    for stk in stklst:
                        if stk>=600000:
                            stkcd = 'sh'+str(stk)
                        else:
                            stkcd = 'sz'+'0'*(6-len(str(stk)))+str(stk)
                        cursor.execute('SELECT COUNT(*) FROM stkmin_{0} WHERE date={1}'.format(stkcd,date))
                        obsnum = cursor.fetchall()[0][0]
                        if obsnum>0:
                            print('{0} pathched on {1} with {2} obs'.format(stkcd,date,obsnum))
                        else:
                            print('{0} still missed on {1}'.format(stkcd,date))
                            miss.append(stk)
                    if len(miss)>1:
                        writer.writerow(miss)

    def _historical_stks(self,enddate):
        dbname='stocks_data_min_by_day'
        self.connectDB(dbname=dbname)
        alltables = self._get_db_tables_all(dbname=dbname)
        conn = self._getConn_()
        cursor = conn.cursor()
        trddates = self._get_trddates(conn)
        datelst = [dt for dt in trddates if dt>=19990726 and dt<=enddate]
        allstk = set([])
        start = time.time()
        ms = open('missing_dates.txt','w')
        with open('historical_stklst.csv','w',newline='') as f:
            writer = csv.writer(f)
            for dt in datelst:
                print(dt)
                if 'stkmin_{0}'.format(dt) not in alltables:
                    print('missing trddate {0}'.format(dt))
                    ms.writelines(str(dt)+'\n')
                    continue
                exeline = 'SELECT DISTINCT stkcd FROM stkmin_{0} WHERE time IN (1000,1100,1330,1430)'.format(dt)
                cursor.execute(exeline)
                stklst = set([stk[0] for stk in cursor.fetchall()])
                newstk = list(stklst - allstk)
                newstk.insert(0,dt)
                allstk |= stklst
                writer.writerow(newstk)
        print(time.time()-start)

    ####################################################################################################################
    ##############################  数据库更新   ####################################################
    ####################################################################################################################
    def update_data_by_day(self,seed=None):
        """ 按日度更新数据，目前为.mat格式 """
        dbname='stocks_data_min_by_day'
        self._switchDB_(seed=seed,dbname=dbname)
        conn = self._getConn_(seed=seed)
        cursor = conn.cursor()
        ######## 提取需要更新的日期 ###############
        datelst = [date.split('.')[0] for date in os.listdir(self._updtpath)]
        newdates = sorted(set(datelst) - set([tb.split('_')[1] for tb in self._get_db_tables_all(dbname=dbname,seed=seed)]))
        if not newdates:
            print('no new table to update for database {0}'.format(dbname))
            return
        else:
            print('{0} tables to update for database {1}'.format(len(newdates),dbname))
        colnames = ['stkcd','time','open','high','low','close','volume','amount','stkid']
        colinfo = ByDay.colinfo
        prmkey = ByDay.prmkey
        for newdt in newdates:
            tablename = 'stkmin_'+newdt
            print(tablename)
            newdata = pd.DataFrame(np.transpose(h5py.File(os.path.join(self._updtpath,'{0}.mat'.format(newdt)))['sdata']),columns=colnames)
            newdata['volamtflag'] = 0
            newdata.loc[newdata['amount']>(newdata['volume']+100)*newdata['high'],'volamtflag'] = 1
            newdata.loc[newdata['amount']<(newdata['volume']-100)*newdata['low'],'volamtflag'] = 2
            newdata.loc[(newdata['amount']==0) & (newdata['volume']>0),'volamtflag'] = 3
            newdata.loc[(newdata['amount']==0) & (newdata['volume']==0),'volamtflag'] = 4
            newdata.drop(['stkid'],axis=1,inplace=True)
            newdata['return'] = newdata.groupby(['stkcd'])['close'].diff()/newdata['close'].shift()
            pal = self._get_mat(theDate=int(newdt)).set_index('stkcd')
            stkHeadIdx = newdata['stkcd'].diff()!=0
            stkHead = newdata[stkHeadIdx].set_index('stkcd')
            coIndex = list(set(pal.index) & set(stkHead.index))
            #   计算第一分钟
            min1Ret = (1 + pal.loc[coIndex, 'pctchg']) / (pal.loc[coIndex, 'close'] / stkHead.loc[coIndex, 'close']) - 1
            stkHead.loc[coIndex, 'return'] = min1Ret.loc[coIndex]
            #   补回第一分钟
            newdata.loc[np.argwhere(stkHeadIdx.values)[:, 0], 'return'] = stkHead['return'].values
            newdata.loc[np.argwhere(np.isinf(newdata['return']).values | np.isnan(newdata['return']).values)[:,0], 'return'] = 0
            self.update_db(conn=conn,data=newdata.values,tablename=tablename,colinfo=colinfo,prmkey=prmkey,dbname=dbname,if_exist='replace')
            retToInsert = '({i},)'
            dateupdt = 'INSERT INTO trddates (date) VALUES ({0})'.format(newdt)
            cursor.execute(dateupdt)
            conn.commit()

    def update_data_by_stock(self,tempfolder,seed=None):
        """ 按股票更新（历史）数据，目前为CSV格式"""
        dbname = 'stocks_data_min_by_stock'
        self._switchDB_(seed=seed,dbname=dbname)
        conn = self._getConn_(seed=seed)
        filepath = os.path.join(self._histpath,tempfolder)
        assert os.path.exists(filepath)
        filelst = self._get_filelst(filepath=filepath,seed=seed)
        print(filelst)
        colnames = ['date','time','open','high','low','close','volume','amount']
        colinfo = ByStock.colinfo
        prmkey = ByStock.prmkey
        updtedlstpath = os.path.join(self._tmpupdt,'updtedlst{0}.txt'.format(seed))
        if not os.path.exists(updtedlstpath):
            os.system('cd.>{0}'.format(updtedlstpath))
        for fl in filelst:
            flname = fl.split('.')[0]
            assert(len(flname))==8
            tablename = 'stkmin_' + flname.lower()
            with open(updtedlstpath,'r') as tmpupdt:  # 处理中途失败的情况
                updtedlst = tmpupdt.readlines()
                updtedlst = [ufl.strip() for ufl in updtedlst]
            if fl in updtedlst:
                continue
            cond1 = not flname[2:].isnumeric()
            cond2 = flname[0:2]=='SH' and (flname[2] not in ('6'))
            cond3 = flname[0:2]=='SZ' and (flname[2] not in ('0','3'))
            cond4 = flname[0:2]=='SZ' and (flname[2:5]=='399')
            cond5 = flname[0:2]=='SZ' and (flname[2:4] in ('08','03'))
            if cond1 or cond2 or cond3 or cond4 or cond5:
                continue
            fldata = pd.read_csv(os.path.join(filepath,fl),names=colnames)
            fldata['stkcd'] = int(flname[2:8])
            fldata['date'] = fldata['date'].str.replace('([/-]?)','').map(int)
            fldata['time'] = fldata['time'].str.replace(':','').map(int).map(lambda x:x if x<10000 else int(x/100))
            for dumi in range(1,len(fldata['time'])): # 重复时间处理
                if fldata['time'][dumi]==fldata['time'][dumi-1]:
                    mins = fldata['time'][dumi]%100
                    hour = (fldata['time'][dumi]-mins)/100
                    hour += (mins==59)
                    mins = 0 if mins==59 else mins+1
                    fldata['time'][dumi] = int(hour*100 + mins)
            fldata['volamtflag'] = 0
            fldata.loc[fldata['amount']>(fldata['volume']+100)*fldata['high'],'volamtflag'] = 1
            fldata.loc[fldata['amount']<(fldata['volume']-100)*fldata['low'],'volamtflag'] = 2
            fldata.loc[(fldata['amount']==0) & (fldata['volume']>0),'volamtflag'] = 3
            fldata.loc[(fldata['amount']==0) & (fldata['volume']==0),'volamtflag'] = 4
            self.update_db(conn=conn,data=fldata.values,tablename=tablename,colinfo=colinfo,prmkey=prmkey,dbname=dbname,if_exist='append')
            with open(updtedlstpath,'a+') as tmpupdt:
                tmpupdt.writelines(fl+'\n')
        os.system('del {0}'.format(updtedlstpath))

    def multi_update_data_by_stock(self,tempfolder):
        """多进程更新 按股票"""
        assert self._corenum>1 # 只在多进程情况下可调用该函数
        dbname = 'stocks_data_min_by_stock'
        self.connectDB(dbname=dbname)
        print('{0}Updating database {1} with {2} cores from {3}'.format(LogMark.info,dbname,self._corenum,tempfolder))
        print()
        pool = mpr.Pool(self._corenum)
        for seed in range(self._corenum):
            args = (tempfolder,seed)
            pool.apply_async(func=self.update_data_by_stock,args=args)
        pool.close()
        pool.join()

    def _oneday_stk2day_(self,day,stklst,seed,writeconn,lock):
        self._switchDB_(seed=seed,dbname='stocks_data_min_by_stock')
        conn = self._getConn_(seed=seed)
        cursor = conn.cursor()
        cols = ['date','time','open','high','low','close','volume','amount','stkcd','volamtflag']
        outcols = ['stkcd','time','open','high','low','close','volume','amount','volamtflag']
        data = pd.DataFrame(columns=cols)
        for stk in stklst:
            cursor.execute('SELECT * FROM {0} WHERE date={1}'.format(stk,day))
            templst = cursor.fetchall()
            data = data.append(pd.DataFrame(templst,columns=cols),ignore_index=True)
        data = data.loc[:,outcols].values
        print('seed {0} : {1} data extracted'.format(seed,data.shape[0]))
        if data.shape[0]==0:
            return
        colinfo = {
            TableCol.stkcd:'INT(6) UNSIGNED NOT NULL',
            TableCol.time:'INT(6) UNSIGNED NOT NULL',
            TableCol.open:'FLOAT',
            TableCol.high:'FLOAT',
            TableCol.low:'FLOAT',
            TableCol.close:'FLOAT',
            TableCol.volume:'DOUBLE',
            TableCol.amount:'DOUBLE',
            TableCol.volamtflag:'INT(1) UNSIGNED NOT NULL'
        }
        prmkey = [TableCol.stkcd,TableCol.time]
        dbname = 'stocks_data_min_by_day'
        with lock:
            tablename = 'stkmin_'+str(day)
            self.update_db(conn=writeconn,data=data,tablename=tablename,colinfo=colinfo,prmkey=prmkey,dbname=dbname,if_exist='append')

    def bystk2byday(self,dates=None):
        writeconn = mysql.connector.connect(**self._loginfo)
        stklsts = []
        for seed in range(self._corenum):
            stklsts.append(self._get_db_tables_split(dbname='stocks_data_min_by_stock',seed=seed))
        trddates = self._get_trddates(writeconn)
        existdates = [int(tb.split('_')[1]) for tb in self._get_db_tables_all(dbname='stocks_data_min_by_day')]
        checktrd = True
        if dates is None:
            dates = [dt for dt in trddates if dt>=19990726]
            checktrd = False
        newdates = sorted(list(set(dates)-set(existdates)))
        for dt in newdates:
            if checktrd and (dt not in trddates):
                print('{0} is not a trade date'.format(dt))
                continue
            if dt>20171130:
                continue
            print('updating ',dt)
            start = time.time()
            lock = mpr.Lock()
            pros = []
            for seed in range(self._corenum):
                args = (dt,stklsts[seed],seed,writeconn,lock)
                pros.append(mpr.Process(target=self._oneday_stk2day_,args=args))
                pros[seed].start()
            for p in pros:
                p.join()
            print(time.time()-start)

    def _splited_day2stk(self,data,stklst,seed):
        self._switchDB_(seed=seed,dbname='stocks_data_min_by_stock')
        conn = self._getConn_(seed=seed)
        # cursor = conn.cursor()
        for stk in stklst:
            if stk>=600000:
                stkcd = 'sh{0}'.format(stk)
            else:
                if stk<300000:
                    stkstr = str(stk)
                    stkcd = 'sz'+(6-len(stkstr))*'0'+stkstr
                else:
                    stkcd = 'sz{0}'.format(stk)
            tablename = 'stkmin_'+stkcd
            byStockCols = [col for col in ByStock.colinfo]
            self.update_db(conn=conn,
                           dbname='stocks_data_min_by_stock',
                           data=data[data['STKCD']==stk].loc[:,byStockCols].values,
                           tablename=tablename,
                           colinfo=ByStock.colinfo,
                           prmkey=ByStock.prmkey,
                           if_exist='append',
                           chunksize=1000)

    def byday2bystk(self):
        dbname='stocks_data_min_by_stock'
        self.connectDB(dbname=dbname)
        # 用于更新trddates
        bystkconn = mysql.connector.connect(**self._loginfo)
        bystkcursor = bystkconn.cursor()
        bystkcursor.execute('USE stocks_data_min_by_stock')
        bystkcursor.execute('SELECT date FROM trddates')
        bystkdates = set([dt[0] for dt in bystkcursor.fetchall()])
        # 用于读取 byday 数据
        bydayconn = mysql.connector.connect(**self._loginfo)
        bydaycursor = bydayconn.cursor()
        bydaycursor.execute('USE stocks_data_min_by_day')
        bydaycursor.execute('SELECT date FROM trddates')
        bydaydates = set([dt[0] for dt in bydaycursor.fetchall()])
        dates = sorted(list(bydaydates-bystkdates))
        trddates = self._get_trddates(conn=bydayconn)
        if not dates:
            print('No new datess to update')
        else:
            print(dates)
        for dts in dates:
            if dts not in trddates:
                print('{0} is not a trade day'.format(dts))
                continue
            else:
                bydaycursor.execute('SELECT DISTINCT stkcd FROM stkmin_{0}'.format(dts))
                allstklst = [stk[0] for stk in bydaycursor.fetchall()]
                data = pd.read_sql('SELECT * FROM stkmin_{0}'.format(dts),con=bydayconn)
                data['DATE'] = dts
                pros = []
                for seed in range(self._corenum):
                    stklst = [stk for ct,stk in enumerate(allstklst,1) if ct%self._corenum==seed]
                    print(stklst)
                    args = (data,stklst,seed)
                    pros.append(mpr.Process(target=self._splited_day2stk,args=args))
                    pros[seed].start()
                for p in pros:
                    p.join()
            bystkcursor.execute('INSERT INTO trddates (date) VALUES ({0})'.format(dts))
            bystkconn.commit()
            print('{0} updated'.format(dts))


    ########### temp 生成 日度 mat 数据文件 ##############
    def gen_daily_mat(self):
        remotePath = r'\\192.168.1.88\mat_data\by_day'

        dbname='stocks_data_min_by_day'
        self.connectDB(dbname=dbname)
        conn = self._getConn_()
        cursor = conn.cursor()

        cursor.execute('SHOW TABLES')
        savedDbTables = set([tb[0] for tb in cursor.fetchall() if tb[0]!='trddates'])
        savedMatsTables = set([mat.split('.')[0] for mat in os.listdir(remotePath)])
        newTables = savedDbTables - savedMatsTables

        print('{} new tables to update'.format(len(newTables)))
        for tb in sorted(newTables):
            matPath = os.path.join(remotePath,'{}.mat'.format(tb))
            cursor.execute('SELECT * FROM {}'.format(tb))
            scio.savemat(file_name=matPath,mdict={tb:np.array(cursor.fetchall())},do_compression=True)
            print('[+]Table {} updated'.format(tb))



if __name__=='__main__':
    obj = StocksMinDB(configpath=r'E:\stocks_data_min\StocksMinDB\configs')
    # obj.gen_daily_mat()
    # obj.update_data_by_day()
    # obj.bystk2byday(dates=[19991008])
    obj._patch_rets_by_day(trdDates=[20080305,20080306])
    # obj._get_mat(theStock=1)
    # obj._get_mat(theDate=20171201)
    # obj._patch_rets_by_stock(allTables=['stkmin_sh603706'])