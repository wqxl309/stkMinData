# encoding:utf-8
import datetime as dt

from dataMinToDay.minToDay import minDataExtractor


if __name__=='__main__':
    obj = minDataExtractor()


    # 全天 momentums
    exprs = ['SUM(POWER(clsret,2))','SQRT(COUNT(clsret))*SUM(POWER(clsret,3))/POWER(SUM(POWER(clsret,2)),1.5)','COUNT(clsret)*SUM(POWER(clsret,4))/POWER(SUM(POWER(clsret,2)),2)']
    conditions = ['','','']
    obj.update_single_day(exprs=exprs, conditions=conditions, dataName='OneDay_moments')

    obj.update_single_day(exprs=['SUM(volume*clsret)'], conditions=[''], dataName='OneDay_volumeRet')

    # 上下午
    conditions = ['AND (time<1200)', 'AND (time>1200)']
    for key in ['volume','amount','clsret']:
        exprs = ['SUM({})'.format(key) for dumi in range(len(conditions))]
        obj.update_single_day(exprs=exprs,conditions=conditions,dataName='HalfDay_{}'.format(key))
    key = 'clsret'
    exprs = ['STD({})'.format(key) for dumi in range(len(conditions))]
    obj.update_single_day(exprs=exprs, conditions=conditions, dataName='HalfDay_volatility')

    # 按小时
    conditions = ['AND (time<=1030)',
                  'AND (time>1030) AND (time<=1200)',
                  'AND (time>1200) AND (time<=1400)',
                  'AND (time>1400)']
    for key in ['volume','amount','clsret']:
        exprs = ['SUM({})'.format(key) for dumi in range(len(conditions))]
        obj.update_single_day(exprs=exprs,conditions=conditions,dataName='OneHour_{}'.format(key))
    key = 'clsret'
    exprs = ['STD({})'.format(key) for dumi in range(len(conditions))]
    obj.update_single_day(exprs=exprs, conditions=conditions, dataName='OneHour_volatility')

    # 开盘后 收盘前 半小时
    conditions = ['AND (time<=1000)', 'AND (time>1430)']
    for key in ['volume','amount','clsret']:
        exprs = ['SUM({})'.format(key) for dumi in range(len(conditions))]
        obj.update_single_day(exprs=exprs,conditions=conditions,dataName='HalfHourOC_{}'.format(key))
    key = 'clsret'
    exprs = ['STD({})'.format(key) for dumi in range(len(conditions))]
    obj.update_single_day(exprs=exprs, conditions=conditions, dataName='HalfHourOC_volatility')

    conditions = ['AND (time<=1000)', 'AND (time>1430)']
    exprs = ['SUM(volume*clsret)', 'SUM(volume*clsret)']
    obj.update_single_day(exprs=exprs, conditions=conditions, dataName='HalfHourOC_volumeRet')


    exprs = [ 'SUM((ABS(open-close)/(high-low))*volume)', 'SUM((ABS(open-close)/(high-low))*volume)']
    conditions = ['AND open>close AND high>low', 'AND open<close AND high>low']
    obj.update_single_day(exprs=exprs, conditions=conditions, dataName='OneDay_consistent')

    # 按照分钟 成交额划分
    cut = [1000000,500000,1000000,5000000]
    conditions = ['AND amount<={}'.format(cut[0]),
                  'AND (amount>{0} AND amount<={1})'.format(cut[0], cut[1]),
                  'AND (amount>{0} AND amount<={1})'.format(cut[1], cut[2]),
                  'AND (amount>{0} AND amount<={1})'.format(cut[2], cut[3]),
                  'AND amount>{}'.format(cut[3])]
    for key in ['volume', 'amount']:
        exprs = ['SUM({})'.format(key) for dumi in range(len(conditions))]
        obj.update_single_day(exprs=exprs, conditions=conditions, dataName='OneDayMoney_{}'.format(key))