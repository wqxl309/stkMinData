
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

class ByStock:
    colinfo = {
        TableCol.date:'INT UNSIGNED NOT NULL',
        TableCol.time:'INT UNSIGNED NOT NULL',
        TableCol.open:'FLOAT',
        TableCol.high:'FLOAT',
        TableCol.low:'FLOAT',
        TableCol.close:'FLOAT',
        TableCol.volume:'DOUBLE',
        TableCol.amount:'DOUBLE',
        TableCol.stkcd:'INT UNSIGNED NOT NULL',
        TableCol.volamtflag:'INT(1) UNSIGNED NOT NULL'
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
        TableCol.stkid:'INT UNSIGNED NOT NULL',
        TableCol.volamtflag:'INT(1) UNSIGNED NOT NULL'
    }
    prmkey = [TableCol.stkcd,TableCol.time]