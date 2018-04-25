import asyncio
import time
import multiprocessing as mpr
import pandas as pd
import scipy.io as scio
import csv

from StocksMinDB.StocksMinDb import StocksMinDB



class StocksMinDbChecker(StocksMinDB):

    def __init__(self,configpath,corenum=1):
        super(StocksMinDbChecker,self).__init__(configpath=configpath,
                                                corenum=corenum
                                                )

    def _get_db_tables_splited(self,dbname,seed=None):
        if self._currdb!=dbname:
            self._db_connect(dbname=dbname)
        total_tbs = self._get_db_tables_all(dbname=dbname,seed=seed)
        if seed is None:
            return total_tbs
        else:
            return [val for ct,val in enumerate(total_tbs,1) if ct%self._corenum==seed]

    def _check_tables(self,dbname,tablelst,seed=None):
        start = time.time()
        if self._currdb != dbname:
            self._db_connect(dbname=dbname)
        # conn = self.conn if seed is None else self.conns[seed]
        cursor = self.cursor if seed is None else self.cursors[seed]
        with open('stklst_vol1.txt','w') as f:
            for tb in tablelst:
                print('checking %s' %tb)
                #cond1 = '(volume=0 and amount>high*100 and high>0)' # 成交量不足100股
                #cond2 = '(date<>20041123 AND volume>0 AND amount>IF(volume+100>volume*1.05,volume+100,volume*1.05)*high)'
                cond3 = 'volume<0 or amount<0'
                cursor.execute('SELECT count(date) FROM {0} WHERE {1}'.format(tb,cond3))
                all = cursor.fetchall()
                if len(all)>0:
                    num = all[0][0]
                    print(tb,num)
                    if num>0:
                        f.writelines(tb+'\n')
        print('{0} : tables check finished with {1} seconds'.format(seed,time.time()-start))

    def check_db(self,dbname):
        start = time.time()
        if self._corenum>1:
            pool = mpr.Pool(self._corenum)
            for seed in range(self._corenum):
                tables = self._get_db_tables_splited(dbname=dbname,seed=seed)
                pool.apply_async(func=self._check_tables,args=(dbname,tables,seed))
            pool.close()
            pool.join()
        else:
            tables = self._get_db_tables_all(dbname=dbname)
            self._check_tables(dbname=dbname,tablelst=tables)
        print('database {0} check finished with {1} seconds'.format(dbname,time.time()-start))


    def update_tables(self,dbname,tablelst=None,seed=None):
        print(tablelst)
        self._switchDB_(seed=seed,dbname=dbname)
        conn = self._getConn_(seed=seed)
        cursor = conn.cursor()
        if tablelst is None:
            tablelst = self._get_db_tables_all(dbname=dbname)
        for tb in tablelst:
            start = time.time()
            tmpval = int(tb.split('_')[1])
            if tmpval<20171201 or tmpval>=20180105:
                continue
            else:
                tb = tb.strip()
                # cursor.execute('ALTER TABLE {0} CHANGE COLUMN volamtflag VOLAMTFLAG INT(1) DEFAULT 0'.format(tb))
                # cursor.execute('UPDATE {0} SET volamtflag=1 WHERE amount>(volume+100)*high'.format(tb))
                # cursor.execute('UPDATE {0} SET volamtflag=2 WHERE amount<(volume-100)*low'.format(tb))
                # cursor.execute('UPDATE {0} SET volamtflag=3 WHERE volume>0 AND amount=0 '.format(tb))
                # cursor.execute('UPDATE {0} SET volamtflag=4 WHERE volume=0 AND amount=0 '.format(tb))
                # conn.commit()
                print(tb,time.time()-start)

    def multi_update_tables(self,dbname):
        start=time.time()
        stklsts = []
        for seed in range(self._corenum):
            stklsts.append(self._get_db_tables_split(dbname=dbname,seed=seed))
        pros = []
        for seed in range(self._corenum):
            args = (dbname,stklsts[seed],seed)
            pros.append(mpr.Process(target=self.update_tables,args=args))
            pros[seed].start()
        for p in pros:
            p.join()
        print(time.time()-start)

    def check_diff_stk(self):
        daily = pd.DataFrame(scio.loadmat(r'stkipos.mat')['stkipos'],columns=['stkcd','ipodate'])
        ipodates = daily.loc[~daily['ipodate'].duplicated(),'ipodate']
        dayloststk = csv.writer(open(r'day_loststk.csv','w',newline=''))
        minloststk = csv.writer(open(r'min_loststk.csv','w',newline=''))
        with open(r'historical_stklst.csv') as f:
            reader = csv.reader(f)
            allstklst = set([])
            for line in reader:
                date = int(line[0])
                stklst = [int(stk) for stk in line[1:] if not (stk[0:2] in ('80','82') and len(stk)==5)]
                allstklst |= set(stklst)
                print('minstknum',len(allstklst))
                if date in ipodates.values:
                    daystks = set(daily.loc[daily['ipodate']<=date,'stkcd'].values)
                    daylost = list(allstklst - daystks)
                    minlost = list(daystks - allstklst)
                    daylost.insert(0,date)
                    minlost.insert(0,date)
                    dayloststk.writerow(daylost)
                    minloststk.writerow(minlost)