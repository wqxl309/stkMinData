
import logging

class LogMark:
    critical = '[=]'
    error = '[-]'
    warning = '[!]'
    info = '[+]'
    debug = '[*]'
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")

class TableCol:
    date = 'DATE'
    stkcd = 'STKCD'
    time = 'TIME'
    open = 'OPEN'
    high = 'HIGH'
    low = 'LOW'
    close = 'CLOSE'
    volume = 'VOLUME'
    amount = 'AMOUNT'
    stkid = 'STKID'
    volamtflag = 'VOLAMTFLAG'
    ret = 'CLSRET'

class ByStock:
    colinfo = {
        # TableCol.stkcd:'INT UNSIGNED NOT NULL',
        TableCol.date:'INT UNSIGNED NOT NULL',
        TableCol.time:'INT UNSIGNED NOT NULL',
        TableCol.open:'FLOAT',
        TableCol.high:'FLOAT',
        TableCol.low:'FLOAT',
        TableCol.close:'FLOAT',
        TableCol.volume:'DOUBLE',
        TableCol.amount:'DOUBLE',
        TableCol.volamtflag:'INT(1) UNSIGNED NOT NULL',
        TableCol.ret:'FLOAT'
    }
    prmkey = [TableCol.date,TableCol.time]

class ByDay:
    colinfo = {
        TableCol.stkcd:'INT UNSIGNED NOT NULL',
        TableCol.time:'INT(6) UNSIGNED NOT NULL',
        TableCol.open:'FLOAT',
        TableCol.high:'FLOAT',
        TableCol.low:'FLOAT',
        TableCol.close:'FLOAT',
        TableCol.volume:'DOUBLE',
        TableCol.amount:'DOUBLE',
        # TableCol.stkid:'INT UNSIGNED NOT NULL',
        TableCol.volamtflag:'INT(1) UNSIGNED NOT NULL',
        TableCol.ret:'FLOAT'
    }
    prmkey = [TableCol.stkcd,TableCol.time]

class DB_NOTES:
    DB_FIRST_DATE = 19990726
    DB_MISSING_DATE = [19990825, 20010704, 20011022, 20020225, 20020227, 20061017, 20070330]
    DB_MISSING_STOCK = [508]    # .mat 中有 分钟数据库中没有的股票
    MAT_MISSING_STOCK = [600849]   # .mat 中没有 分钟数据库中有的股票