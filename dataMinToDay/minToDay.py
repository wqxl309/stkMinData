import os
import sys
import time

import configparser as cp
import numpy as np
import scipy.io as scio
import mysql.connector

import StocksMinDB.Constants as dbConstants
from dataMinToDay.Constants import dataConstants


class minDataExtractor:

    def stkcd_int_trans(self,stkint):
        stkstr = str(stkint)
        if stkint>=600000:
            stkstr = 'sh'+stkstr
        else:
            stklen = len(stkstr)
            if stklen<6:
                stkstr = 'sz'+'0'*(6-stklen)+stkstr
            else:
                stkstr = 'sz'+stkstr
        return stkstr


    def __init__(self,config='.\configs'):
        cfp = cp.ConfigParser()
        cfp.read(os.path.join(config,'pathInfo.ini'))
        self._dataPath = cfp.get('Folders','data')
        self._dates = scio.loadmat(os.path.join(cfp.get('Folders','trddates'),'trddates.mat'))['trddates'][:,0]
        stkinfo = scio.loadmat(os.path.join(cfp.get('Folders','stkinfo'),'stkinfo.mat'))['stkinfo']
        self._stkcds = stkinfo[:,0]
        self._stkipos = stkinfo[:,1]
        cfp.read(os.path.join(config,'loginInfo.ini'))
        self._loginfo = dict(cfp.items('login'))

    def _select_by_day(self,cursor,date,stkList,expr,condition):
        paras = {'expr':expr,
                 'condition':condition if condition is not None else '',
                 'date':date,
                 'stkList':'('+','.join([str(stk) for stk in stkList])+')'}
        exeLine = 'SELECT DISTINCT stkcd,{expr} FROM stkmin_{date} WHERE (stkcd IN {stkList}) {condition} GROUP BY stkcd'.format(**paras)
        cursor.execute(exeLine)
        result = cursor.fetchall()
        return np.array(result) if result else None

    def _select_by_stk(self,cursor,stkcd,dateList,expr,condition):
        paras = {'expr':expr,
                 'condition':condition if condition is not None else '',
                 'stkstr':self.stkcd_int_trans(stkint=stkcd),
                 'dateList':'('+','.join([str(date) for date in dateList])+')'}
        exeLine = 'SELECT DISTINCT date,{expr} FROM stkmin_{stkstr} WHERE (date IN {dateList}) {condition} GROUP BY date'.format(**paras)
        cursor.execute(exeLine)
        result = cursor.fetchall()
        return np.array(result) if result else None

    def _extract_data(self,cursor,dateList,stkList,exprs,conditions,exprPages=None,byAxis=None):
        """
        :param cursor:
        :param dateList:
        :param stkList:
        :param exprs:
        :param exprPages:  针对expr中包含多列的情况，ex 'sum(a) as a, sum(b)', 制定需要返回的列 exprPage = [False,True]
        :param conditions:
        :param byAxis:
        :return: output # page 0 is the flag page, 通过二进制数来标记，def : sum( 2** [expr1valid, expr2valid...] )  exprivalid = 1 if valid else 0
        """
        start = time.time()
        dayNum = dateList.shape[0]
        stkNum = stkList.shape[0]
        if byAxis is None:
            byAxis = 'stkcd' if stkNum<dayNum else 'day'
        dbName = 'stocks_data_min_by_{}'.format('stock' if byAxis=='stkcd' else 'day')
        cursor.execute('USE {}'.format(dbName))
        exprPages = [[True for dumi in expr.split(' , ')] for expr in exprs] if exprPages is None else exprPages
        # set up output
        assert(len(exprs)==len(conditions))
        pageNum = np.sum([sum(epg) for epg in exprPages])
        output = np.zeros([stkNum,dayNum,pageNum+1])
        if byAxis=='stkcd':
            for dumi,stk in enumerate(stkList):
                if stk in dataConstants.DB_MISSING_STOCK:
                    continue
                s1 = time.time()
                exprTotPages = 0
                for exprCnt,expr in enumerate(exprs):
                    condition = conditions[exprCnt]
                    exprPage = exprPages[exprCnt]
                    oneData = self._select_by_stk(cursor=cursor,stkcd=stk,dateList=dateList,expr=expr,condition=condition)
                    if oneData is not None:
                        validIdx = np.isin(dateList,oneData[:,0],assume_unique=True)    # 每日数据的第一列 输出日期
                        output[dumi,:,0] += validIdx*(2**exprCnt)                       # 更新二进制 flag
                        pageCnt = np.cumsum(exprPage)                         # 当前expr中，需要存储的page 的位置
                        for page in pageCnt:
                            output[dumi,validIdx,exprTotPages + page] = oneData[:,page]
                        exprTotPages += len(exprPages[exprCnt])
                print('stock {0} processed with {1} seconds'.format(stk,time.time()-s1))
        else:
            for dumi,day in enumerate(dateList):
                if (day < dataConstants.DB_FIRST_DATE) or (day in dataConstants.DB_MISSING_DATE):
                    continue
                s1 = time.time()
                exprTotPages = 0
                for exprCnt, expr in enumerate(exprs):
                    condition = conditions[exprCnt]
                    exprPage = exprPages[exprCnt]
                    oneData = self._select_by_day(cursor=cursor,date=day,stkList=stkList,expr=expr,condition=condition)
                    if oneData is not None:
                        validIdx = np.isin(stkList,oneData[:,0],assume_unique=True)     # 每日数据的第一列 输出股票代码
                        output[:,dumi,0] += validIdx*(2**exprCnt)
                        pageCnt = np.cumsum(exprPage)  # 当前expr中，需要存储的page 的位置
                        for page in pageCnt:
                            output[validIdx,dumi,exprTotPages + page] = oneData[:,page]
                        exprTotPages += len(exprPages[exprCnt])
                print('date {0} processed with {1} seconds'.format(day,time.time()-s1))
        print('all processed with {} seconds'.format(time.time() - start))
        return output

    def update_single_day(self,dataName,exprs,conditions,exprPages=None):
        """
            提取 单独一个叫日内可生成的数据
            数据应存储为 形状同Pal
        """
        conn = mysql.connector.connect(**self._loginfo)
        cursor = conn.cursor()

        dataPath = os.path.join(self._dataPath,dataName)
        histMatPath = os.path.join(dataPath,dataConstants.HISTMAT+'.mat')
        currMatPath = os.path.join(dataPath,dataConstants.CURRMAT+'.mat')
        if os.path.exists(dataPath):
            newHist = not os.path.exists(histMatPath)
            newCurr = not os.path.exists(currMatPath)
        else:
            os.mkdir(dataPath)
            newHist = True
            newCurr = True
        if newHist:     # 创建历史 mat
            # 创建配置文件
            configPath = os.path.join(dataPath,'mat_data.ini')
            with open(configPath,'w') as cf:
                cf.writelines('[{}]\n'.format(dataName))
                cf.writelines('slice = 2\n')
                cf.writelines('dimData = 1\n')
            histData = self._extract_data(cursor=cursor,byAxis='stkcd',dateList=self._dates[:dataConstants.HIST_DAYNUM],stkList=self._stkcds[:dataConstants.HIST_STKNUM],exprs=exprs,conditions=conditions,exprPages=exprPages)
            scio.savemat(file_name=histMatPath,mdict={dataName:histData})
            print('hist mat created')
        if newCurr:     # 创建当前 mat
            histMat = scio.loadmat(histMatPath)[dataName]
            currPartHist = histMat[:,(dataConstants.CUT_DATENUM-1):]
            scio.savemat(file_name=currMatPath,mdict={dataName:currPartHist})
            print('curr mat created')
        # 检查并更新 currmat
        currDates = self._dates[(dataConstants.CUT_DATENUM-1):]
        currStkcds = self._stkcds
        currDayNum = currDates.shape[0]
        currStkNum = currStkcds.shape[0]
        currMatSaved = scio.loadmat(currMatPath)[dataName]
        (savedStkNum,savedDayNum,pageNum) = currMatSaved.shape
        if (currDayNum==savedDayNum) and (currStkNum==savedStkNum):
            print('no data to update')
            return
        patch = np.zeros((currStkNum-savedStkNum,savedDayNum,pageNum))
        currUpdate = self._extract_data(cursor=cursor,dateList=currDates[savedDayNum:],stkList=currStkcds,exprs=exprs,conditions=conditions,exprPages=exprPages)
        currMat = np.column_stack([np.row_stack([currMatSaved,patch]),currUpdate])
        scio.savemat(file_name=currMatPath,mdict={dataName:currMat})
        print('curr mat updated successfully, with {0} stocks and {1} days updated'.format(currStkNum-savedStkNum,currDayNum-savedDayNum))

