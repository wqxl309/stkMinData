# encoding:utf-8
import datetime as dt

from dataMinToDay.minToDay import minDataExtractor


if __name__=='__main__':
    obj = minDataExtractor()

    # 上下午
    for key in ['volume','amount','clsret']:
        exprs = ['SUM({})'.format(key),'SUM({})'.format(key)]
        conditions = ['AND (time<1200)','AND (time>1200)']
        obj.update_single_day(exprs=exprs,conditions=conditions,dataName='MorningAfternoon_{}'.format(key))
    key = 'clsret'
    exprs = ['STD({})'.format(key), 'STD({})'.format(key)]
    conditions = ['AND (time<1200)', 'AND (time>1200)']
    obj.update_single_day(exprs=exprs, conditions=conditions, dataName='MorningAfternoon_volatility')

    # 开盘后 收盘前 半小时
    for key in ['volume','amount','clsret']:
        exprs = ['SUM({})'.format(key),'SUM({})'.format(key)]
        conditions = ['AND (time<=1000)','AND (time>1430)']
        obj.update_single_day(exprs=exprs,conditions=conditions,dataName='HalfHourOC_{}'.format(key))
    key = 'clsret'
    conditions = ['AND (time<=1000)', 'AND (time>1430)']
    obj.update_single_day(exprs=exprs, conditions=conditions, dataName='HalfHourOC_volatility')

    # 全天 momentums
    exprs = ['SUM(POWER(clsret,2))','SQRT(COUNT(clsret))*SUM(POWER(clsret,3))/POWER(SUM(POWER(clsret,2)),1.5)','COUNT(clsret)*SUM(POWER(clsret,4))/POWER(SUM(POWER(clsret,2)),2)']
    conditions = ['','','']
    obj.update_single_day(exprs=exprs, conditions=conditions, dataName='OneDay_moments')
