import datetime as dt
import os

import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import sys
sys.path.append(r'E:\stocks_data_min')
from StocksMinDB.StocksMinDb import StocksMinDB
from StocksMinDB.CheckDatabases import databaseChecker



if __name__=='__main__':
    todaystr = dt.datetime.today().strftime('%Y%m%d')

    remotedir = r'\\Zhouhu\a股历史数据\A股分钟数据每日更新\stockdata1F'
    localdir = r'E:\stocks_data\min_data\stockdata1F'
    newdates = set(os.listdir(remotedir)) - set(os.listdir(localdir))
    if not newdates:
        print('No new data to update on date {}'.format(todaystr))
    else:
        print('{0} days to copy'.format(len(newdates)))
        for dt in newdates:
            os.system('COPY {0} {1}'.format(os.path.join(remotedir,dt),os.path.join(localdir,dt)))

    ############### daily update ###############
    config = r'E:\stocks_data_min\StocksMinDB\configs'

    print('updating by_day')
    # 1st date: 19990726
    # Columns: STKCD TIME OPEN HIGH LOW CLOSE VOLUME AMOUNT VOLAMTFLAG
    obj = StocksMinDB(configpath=config,corenum=1)
    obj.update_data_by_day()
    print()

    print('updating by_stock')
    # Columns: DATE TIME OPEN HIGH LOW CLOSE VOLUME AMOUNT STKCD VOLAMTFLAG
    obj = StocksMinDB(configpath=config,corenum=10)
    obj.byday2bystk()

    ### check updated data ###
    checker = databaseChecker()
    reportPath = r'E:\stocks_data_min\StocksMinDB\check_reports'
    preCheckReports = os.listdir(reportPath)
    for newdt in newdates:
        newdtstr = newdt.split('.')[0]
        checker.check_update(checkDate=newdtstr,outputPath=reportPath)
    aftCheckReports = os.listdir(reportPath)
    newReports = set(aftCheckReports) - set(preCheckReports)    # 检查是否有问题报告

    # 生成邮件
    message = MIMEMultipart()
    message['From'] = Header("百泉投资", 'utf-8')
    #message['To'] =  Header("测试", 'utf-8')
    message['Subject'] = Header('分钟数据库更新简报_{0}'.format(todaystr), 'utf-8')
    #添加邮件正文内容
    if newReports:
        mailTxt = 'Errors found during check, see attachment for report ({} reports)'.format(len(newReports))
        for rpt in newReports:
            # 添加附件
            rptName = rpt.split('.')[0]
            attfile = MIMEText(open(rpt, 'rb').read(), 'base64', 'utf-8')
            attfile["Content-Type"] = 'application/octet-stream'
            attfile["Content-Disposition"] = 'attachment; filename="{0}"'.format(rptName)
            message.attach(attfile)
    else:
        mailTxt = 'All update passed checks,{} days updated'.format(len(newdates))
    message.attach(MIMEText(mailTxt, 'plain', 'utf-8'))
    # 发送邮件
    try:
        sender = 'baiquaninvest@baiquaninvest.com'
        receivers = ['wangjp@baiquaninvest.com']
        smtpobj = smtplib.SMTP()
        smtpobj.connect(host='smtp.qiye.163.com',port=25)
        smtpobj.login(user=sender,password='Baiquan@1818')
        smtpobj.sendmail(sender,receivers,message.as_string())
    except BaseException:
        raise Exception('sending emails failed')