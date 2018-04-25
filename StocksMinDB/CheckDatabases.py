
import datetime as dt
import pandas as pd
import os
import time
import sys
sys.path.append(r'E:\stocks_data_min')
from StocksMinDB.StocksMinDb import StocksMinDB
from StocksMinDB.Constants import ByStock,ByDay
from gmsdk import md

class databaseChecker:

    def __init__(self,configPath = r'E:\stocks_data_min\StocksMinDB\configs'):
        self.byDayDb = StocksMinDB(configpath=configPath,corenum=1)
        self.byDayDb.connectDB('stocks_data_min_by_day')
        self.byStkDb = StocksMinDB(configpath=configPath,corenum=1)
        self.byStkDb.connectDB('stocks_data_min_by_stock')
        md.init('18201141877','Wqxl7309')

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

    def check_update(self,checkDate,outputPath = r'E:\stocks_data_min\StocksMinDB\check_reports'):
        """
            对比 checkDate 对应的两个数据库，应满足:
            当天股票数量一致
            每只股票 Kbar 数量一致
            两个库的交易日期一致
            输出：report
            当日较前日比，增加的新股
            当日股票数量
            是否有更新失败的股票，如果有是哪些
        """
        start = time.time()
        if isinstance(checkDate,dt.datetime):
            checkDate = checkDate.strftime('%Y%m%d')
        today = dt.datetime.today().strftime('%Y%m%d')
        assert checkDate>='19990726' and checkDate<=today
        print('***************************')
        print('Checking data base on date {}'.format(checkDate))
        print('***************************')
        bars = md.get_last_n_dailybars('SHSE.000001', 2, end_time=checkDate)
        preDate = ''.join(bars[1].strtime.split('T')[0].split('-'))
        ###### check by_day  ######
        connByDay = self.byDayDb._getConn_()
        cursorByDay = connByDay.cursor()
        ### trd dates ###
        cursorByDay.execute('SELECT date FROM trddates')
        byDayTrddates = set([trd[0] for trd in cursorByDay.fetchall()])
        cursorByDay.execute('SELECT stkcd,count(*) FROM stkmin_{} GROUP BY stkcd'.format(checkDate))
        ### stocks ###
        byDayBars = pd.DataFrame(cursorByDay.fetchall(),columns=['stkcd','barnum'])
        checkDateStocks = set(byDayBars['stkcd'].values)
        if checkDate>'19990726':
            cursorByDay.execute('SELECT DISTINCT stkcd FROM stkmin_{}'.format(preDate))
            preDateStocks = set([stk[0] for stk in cursorByDay.fetchall()])
        else:
            preDateStocks = set()
        newStocks = checkDateStocks - preDateStocks
        print('\n[+]{0} new stocks listed on date {1}'.format(len(newStocks),checkDate))
        print(newStocks)
        ###### check by_stk  ######
        connByStk = self.byStkDb._getConn_()
        cursorByStk = connByStk.cursor()
        ### trd dates ###
        cursorByStk.execute('SELECT date FROM trddates')
        byStkTrddates = set([trd[0] for trd in cursorByStk.fetchall()])
        moreTrdByDay = byDayTrddates - byStkTrddates
        moreTrdByStk = byStkTrddates - byDayTrddates
        if (moreTrdByDay | moreTrdByStk):
            print('\n[-]Different trdate dates')
            print('more in by day:',moreTrdByDay)
            print('more in by stk:',moreTrdByStk)
        else:
            print('\n[+]Trade dates matched between two databases')
        ### stocks ###
        cursorByStk.execute('SHOW TABLES')
        allStocks = set([int(tb[0].split('_')[1][2:]) for tb in cursorByStk.fetchall() if tb[0]!='trddates'])
        lostNewStocks = newStocks - allStocks
        if lostNewStocks:
            print('\n[-]Following new stocks NOT updated in stocks_data_min_by_stock')
            print(lostNewStocks)
        else:
            print('\n[+]New stocks matched between two databases on date {}'.format(checkDate))
        lostCheckDateStocks = checkDateStocks - allStocks
        if lostCheckDateStocks:
            print('\n[-]Following stocks NOT updated in stocks_data_min_by_stock on date {}'.format(checkDate))
            print(lostCheckDateStocks)
        else:
            print('\n[+]All stocks matched between two databases on date {}'.format(checkDate))

        ### check each stock ###
        missBarStocks = []
        for stk in sorted(list(checkDateStocks)):
            # stkstr = str(stk)
            # if stk>=600000:
            #     stkstr = 'sh'+stkstr
            # else:
            #     stklen = len(stkstr)
            #     if stklen<6:
            #         stkstr = 'sz'+'0'*(6-stklen)+stkstr
            #     else:
            #         stkstr = 'sz'+stkstr
            stkstr = self.stkcd_int_trans(stkint=stk)
            cursorByStk.execute('SELECT count(*) FROM stkmin_{0} WHERE date={1}'.format(stkstr,checkDate))
            barnumByStk = cursorByStk.fetchall()[0][0]
            barnumByDay = byDayBars.loc[byDayBars['stkcd']==stk,'barnum'].values[0]
            if barnumByDay==barnumByStk:
                print('[+]Stock {0} bar num matched on date {1}'.format(stkstr,checkDate))
            else:
                missBarStocks.append([stk,barnumByStk,barnumByDay])
        if missBarStocks:
            print('\n[-] Missing bars between two databases on date {}'.format(checkDate))
            missTable = pd.DataFrame(missBarStocks,columns=['stkcd','barnumByStk','barnumByDay'])
            print(missTable)
            missTable.to_csv(os.path.join(outputPath,'check_report_{}.csv'.format(checkDate)),index=False)
        else:
            print('\n[+] All bars of {0} stocks matched between two databases on date {1}'.format(len(checkDateStocks),checkDate))
        print('\nCheck finished on date {0} with {1} seconds'.format(checkDate,time.time()-start))

    def fix_missing(self,checkDate,reportPath = r'E:\stocks_data_min\StocksMinDB\check_reports'):
        """
            根据 check_report，修复其中有问题的股票
        """
        reportFile = os.path.join(reportPath,'check_report_{}.csv'.format(checkDate))
        report = pd.read_csv(reportFile)
        print(report)
        ### take from by day ###
        stkcds = list(report['stkcd'].values)
        stkcdStr = [str(stk) for stk in stkcds]
        connByDay = self.byDayDb._getConn_()
        cursorByDay = connByDay.cursor()
        byStkCol = [col for col in ByStock.colinfo]
        missedData = pd.read_sql('SELECT * FROM stkmin_{0} WHERE stkcd IN ({1})'.format(checkDate,','.join(stkcdStr)),con=connByDay)
        missedData['DATE'] = int(checkDate)
        missedData = missedData.loc[:,byStkCol]
        ### reinsert in to by stock, clear pre first ###
        connByStk = self.byStkDb._getConn_()
        cursorByStk = connByStk.cursor()
        for stk in stkcds:
            stkstr = self.stkcd_int_trans(stkint=stk)
            cursorByStk.execute('DELETE FROM stkmin_{0} WHERE date={1}'.format(stkstr,checkDate))
            print('\n[+]Stock {0} pre-recorders of date {1} cleared'.format(stkstr,checkDate))
            self.byStkDb.update_db(conn=connByStk,
                                   dbname='stocks_data_min_by_stock',
                                   data=missedData[missedData['STKCD']==stk].values,
                                   tablename='stkmin_{0}'.format(stkstr),
                                   colinfo=ByStock.colinfo,
                                   prmkey=ByStock.prmkey,
                                   if_exist='append',
                                   chunksize=1000
                                   )
            print('[+]Stock {0} reupdated of date {1} \n'.format(stkstr,checkDate))
        print('\n[+]All fixed for date {}'.format(checkDate))

if __name__=='__main__':
    checker = databaseChecker()
    checker.check_update(checkDate='20180125')

    # checker.fix_missing(checkDate='20180125')

    # config = r'E:\stocks_data_min\StocksMinDB\configs'
    # obj = StocksMinDB(configpath=config,corenum=1)
    # obj.connectDB(dbname='stocks_data_min_by_day')
    # trddates = obj._get_trddates(conn=obj._getConn_())
    # for trd in trddates:
    #     if trd>=20171201:
    #         checker.check_update(checkDate=str(trd))