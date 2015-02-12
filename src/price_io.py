__author__ = 'grahamcrowell'

import datetime
import urllib2
# import sqlite3
import db_io



""" mysql  DATABASE """
sp500 = ['MMM', 'ABT', 'ABBV', 'ACE', 'ACN', 'ACT', 'ADBE', 'ADT', 'AES', 'AET', 'AFL', 'AMG', 'A', 'GAS', 'APD', 'ARG', 'AKAM', 'AA', 'ALXN', 'ATI', 'ALLE', 'AGN', 'ADS', 'ALL', 'ALTR', 'MO', 'AMZN', 'AEE', 'AEP', 'AXP', 'AIG', 'AMT', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'APC', 'ADI', 'AON', 'APA', 'AIV', 'AAPL', 'AMAT', 'ADM', 'AIZ', 'T', 'ADSK', 'ADP', 'AN', 'AZO', 'AVGO', 'AVB', 'AVY', 'AVP', 'BHI', 'BLL', 'BAC', 'BK', 'BCR', 'BAX', 'BBT', 'BDX', 'BBBY', 'BRK.B', 'BBY', 'BIIB', 'BLK', 'HRB', 'BA', 'BWA', 'BXP', 'BSX', 'BMY', 'BRCM', 'BF.B', 'CHRW', 'CA', 'CVC', 'COG', 'CAM', 'CPB', 'COF', 'CAH', 'CFN', 'KMX', 'CCL', 'CAT', 'CBG', 'CBS', 'CELG', 'CNP', 'CTL', 'CERN', 'CF', 'SCHW', 'CHK', 'CVX', 'CMG', 'CB', 'CI', 'XEC', 'CINF', 'CTAS', 'CSCO', 'C', 'CTXS', 'CLX', 'CME', 'CMS', 'COH', 'KO', 'CCE', 'CTSH', 'CL', 'CMCSA', 'CMA', 'CSC', 'CAG', 'COP', 'CNX', 'ED', 'STZ', 'GLW', 'COST', 'CCI', 'CSX', 'CMI', 'CVS', 'DHI', 'DHR', 'DRI', 'DVA', 'DE', 'DLPH', 'DAL', 'DNR', 'XRAY', 'DVN', 'DO', 'DTV', 'DFS', 'DISCA', 'DISCK', 'DG', 'DLTR', 'D', 'DOV', 'DOW', 'DPS', 'DTE', 'DD', 'DUK', 'DNB', 'ETFC', 'EMN', 'ETN', 'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'EMC', 'EMR', 'ENDP', 'ESV', 'ETR', 'EOG', 'EQT', 'EFX', 'EQR', 'ESS', 'EL', 'EXC', 'EXPE', 'EXPD', 'ESRX', 'XOM', 'FFIV', 'FB', 'FDO', 'FAST', 'FDX', 'FIS', 'FITB', 'FSLR', 'FE', 'FISV', 'FLIR', 'FLS', 'FLR', 'FMC', 'FTI', 'F', 'FOSL', 'BEN', 'FCX', 'FTR', 'GME', 'GCI', 'GPS', 'GRMN', 'GD', 'GE', 'GGP', 'GIS', 'GM', 'GPC', 'GNW', 'GILD', 'GS', 'GT', 'GOOGL', 'GOOG', 'GWW', 'HAL', 'HOG', 'HAR', 'HRS', 'HIG', 'HAS', 'HCA', 'HCP', 'HCN', 'HP', 'HES', 'HPQ', 'HD', 'HON', 'HRL', 'HSP', 'HST', 'HCBK', 'HUM', 'HBAN', 'ITW', 'IR', 'TEG', 'INTC', 'ICE', 'IBM', 'IP', 'IPG', 'IFF', 'INTU', 'ISRG', 'IVZ', 'IRM', 'JEC', 'JNJ', 'JCI', 'JOY', 'JPM', 'JNPR', 'KSU', 'K', 'KEY', 'GMCR', 'KMB', 'KIM', 'KMI', 'KLAC', 'KSS', 'KRFT', 'KR', 'LB', 'LLL', 'LH', 'LRCX', 'LM', 'LEG', 'LEN', 'LVLT', 'LUK', 'LLY', 'LNC', 'LLTC', 'LMT', 'L', 'LO', 'LOW', 'LYB', 'MTB', 'MAC', 'M', 'MNK', 'MRO', 'MPC', 'MAR', 'MMC', 'MAS', 'MA', 'MAT', 'MKC', 'MCD', 'MHFI', 'MCK', 'MJN', 'MWV', 'MDT', 'MRK', 'MET', 'KORS', 'MCHP', 'MU', 'MSFT', 'MHK', 'TAP', 'MDLZ', 'MON', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MUR', 'MYL', 'NBR', 'NDAQ', 'NOV', 'NAVI', 'NTAP', 'NFLX', 'NWL', 'NFX', 'NEM', 'NWSA', 'NEE', 'NLSN', 'NKE', 'NI', 'NE', 'NBL', 'JWN', 'NSC', 'NTRS', 'NOC', 'NU', 'NRG', 'NUE', 'NVDA', 'ORLY', 'OXY', 'OMC', 'OKE', 'ORCL', 'OI', 'PCG', 'PCAR', 'PLL', 'PH', 'PDCO', 'PAYX', 'PNR', 'PBCT', 'POM', 'PEP', 'PKI', 'PRGO', 'PETM', 'PFE', 'PM', 'PSX', 'PNW', 'PXD', 'PBI', 'PCL', 'PNC', 'RL', 'PPG', 'PPL', 'PX', 'PCP', 'PCLN', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PSA', 'PHM', 'PVH', 'QEP', 'PWR', 'QCOM', 'DGX', 'RRC', 'RTN', 'RHT', 'REGN', 'RF', 'RSG', 'RAI', 'RHI', 'ROK', 'COL', 'ROP', 'ROST', 'RCL', 'R', 'CRM', 'SNDK', 'SCG', 'SLB', 'SNI', 'STX', 'SEE', 'SRE', 'SHW', 'SIAL', 'SPG', 'SJM', 'SNA', 'SO', 'LUV', 'SWN', 'SE', 'STJ', 'SWK', 'SPLS', 'SBUX', 'HOT', 'STT', 'SRCL', 'SYK', 'STI', 'SYMC', 'SYY', 'TROW', 'TGT', 'TEL', 'TE', 'THC', 'TDC', 'TSO', 'TXN', 'TXT', 'HSY', 'TRV', 'TMO', 'TIF', 'TWX', 'TWC', 'TJX', 'TMK', 'TSS', 'TSCO', 'RIG', 'TRIP', 'FOXA', 'TSN', 'TYC', 'USB', 'UA', 'UNP', 'UNH', 'UPS', 'MLM', 'URI', 'UTX', 'UHS', 'UNM', 'URBN', 'VFC', 'VLO', 'VAR', 'VTR', 'VRSN', 'VZ', 'VRTX', 'VIAB', 'V', 'VNO', 'VMC', 'WMT', 'WBA', 'DIS', 'WM', 'WAT', 'ANTM', 'WFC', 'WDC', 'WU', 'WY', 'WHR', 'WFM', 'WMB', 'WIN', 'WEC', 'WYN', 'WYNN', 'XEL', 'XRX', 'XLNX', 'XL', 'XYL', 'YHOO', 'YUM', 'ZMH', 'ZION', 'ZTS']

def pretty_datetime(dt):
    if isinstance(dt, datetime.datetime):
        return dt.strftime('%A %B %d %H:%M')
    elif isinstance(dt, datetime.date):
        return dt.strftime('%A %B %d')
    else:
        return datetime.date.fromtimestamp(dt).strftime('%A %B %d')

# sqllite
def upload_price_data(_data):
    '''
    INSERT OR IGNORE upload to database price table
    :param _data: list of tuples which represent rows
    :return: None
    '''
    print('\tuploading {} price data'.format(len(_data)))
    db_io.cur().executemany(db_io.upload_price_stmt, _data)
# MySQL
def upload_price_data(_data):
    '''
    INSERT OR IGNORE upload to database price table
    :param _data: list of tuples which represent rows
    :return: None
    '''
    print('\tuploading {} price data'.format(len(_data)))
    db_io.cur().executemany(db_io.upload_price_stmt, _data)

# sqllite and MySQL
def price_data_cur(_symbol):
    return db_io.con().cursor().execute(db_io.select_price_stmt, (_symbol,))

# sqllite
def load_price_data(_symbol):
    return db_io.cur().execute(db_io.select_price_stmt, (_symbol,)).fetchall()
# MySQL
def load_price_data(_symbol):
    tmp_cur = price_data_cur(_symbol)
    if tmp_cur is not None:
        price_data = [dpt for dpt in tmp_cur]
        tmp_cur.close()
    else:
        price_data = None
    return price_data

# sqllite
def last_price_upload(_symbol):
    dt_str = db_io.cur().execute(db_io.lastup_price_stmt, (_symbol,)).fetchone()[0]
    if dt_str is None:
        return None
    else:
        _format = "%Y-%m-%d"
        return datetime.datetime.strptime(dt_str, _format).date()
# MySQL
def last_price_upload(_symbol):
    tmp_cur = db_io.con().cursor(buffered=True)
    tmp_cur.execute(db_io.lastup_price_stmt, (_symbol,))
    dt = tmp_cur.fetchone()[0]
    tmp_cur.close()
    return dt

# sqllite
def parse_price_line(symbol, line):
    # Date,Open,High,Low,Close,Volume,Adj Close
    # 01/16/2015,45.31,46.28,45.17,46.24,35631100,46.24
    _format = "%m/%d/%Y"
    tkns = line.split(',')
    if '/' in tkns[0]:
        _format = "%m/%d/%Y"
    elif '-' in tkns[0]:
        _format = "%Y-%m-%d"
    dt = datetime.datetime.strptime(tkns[0], _format).date()
    ohlc = list(map(float, tkns[1:5]))
    ohlc.insert(0, dt.isoformat())
    ohlc.insert(0, symbol)
    ohlc.append(int(tkns[5]))
    ohlc.append(float(tkns[6]))
    ohlc = tuple(ohlc)
    return ohlc
# MySQL
def parse_price_line(symbol, line):
    # Date,Open,High,Low,Close,Volume,Adj Close
    # 01/16/2015,45.31,46.28,45.17,46.24,35631100,46.24
    _format = "%m/%d/%Y"
    tkns = line.split(',')
    if '/' in tkns[0]:
        _format = "%m/%d/%Y"
    elif '-' in tkns[0]:
        _format = "%Y-%m-%d"
    dt = datetime.datetime.strptime(tkns[0], _format).date()
    ohlc = list(map(float, tkns[1:5]))
    ohlc.insert(0, dt)
    ohlc.insert(0, symbol)
    ohlc.append(int(tkns[5]))
    ohlc.append(float(tkns[6]))
    ohlc = tuple(ohlc)
    return ohlc
# sqllite and MySQL (only parse_price_line is different)
def download_price_data(_symbol, _date=None):
    _symbol = _symbol.upper()

    if _date is None:
        print('\tdownloading all price data: {}'.format(_symbol))
        _url = r'http://real-chart.finance.yahoo.com/table.csv?s={symbol}&g=d&ignore=.csv'.format(symbol=_symbol)
    else:
        print('\tdownloading partial price data: {} (datetime = {})'.format(_symbol, _date))
        _url = r'http://real-chart.finance.yahoo.com/table.csv?s={symbol}&a={month_i}&b={day_i}&c={year_i}&g=d&ignore=.csv'.format(symbol=_symbol, year_i=_date.year, month_i=_date.month,
                                                                                                                                   day_i=_date.day)
        print(_url)
    # print('\t\t'+_url)
    try:
        _request = urllib2.urlopen(_url)
    except Exception as e:
        if _date is not None:
            print('\t\tno price data of {} available for after {} retrying...'.format(_symbol, _date))
            return download_price_data(_symbol)
        else:
            print('\t\tERROR: no price data available for {}'.format(_symbol))
            return None
    _data = _request.read()
    lines = _data.split('\n')[1:-1]
    _data = list(map(lambda line: parse_price_line(_symbol, line), lines))
    print('\t\t({} OHLCs for {} downloaded)'.format(len(_data),_symbol))
    # arr = np.array(_data, dtype=price_dtype)
    return _data

# sqllite and MySQL
def last_trade_date(_datetime=None):
    # print('last_trade_date(_datetime={})'.format(_datetime))
    if _datetime is None:
        _datetime = datetime.datetime.now()
    elif isinstance(_datetime, datetime.date):
        _datetime = datetime.datetime(year=_datetime.year, month=_datetime.month, day=_datetime.day, hour=16)

    print('computing most recent trade day from: {}'.format(pretty_datetime(_datetime)))
    # print('\tcurrent datetime: {}'.format(_datetime.strftime('%A %B %d %H:%M')))
    tmp_date = None
    # today is a weekday
    if _datetime.weekday() < 5:
        # markets are closed
        if _datetime.hour >= 16:
            tmp_date = _datetime.date()
            # return _datetime.date()
        # elif _datetime.hour <=
        # today is monday
        elif _datetime.weekday() == 0:
            tmp_date =  (_datetime - datetime.timedelta(days=3)).date()
            # return (_datetime - datetime.timedelta(days=3)).date()
    # today is sunday
    elif _datetime.weekday() == 6:
        # print('sunday')
        tmp_date = (_datetime - datetime.timedelta(days=2)).date()
        # return (_datetime - datetime.timedelta(days=2)).date()
    # today is saturday, or markets are open
    # print('last full trade day was yesterday')
    tmp_date = (_datetime - datetime.timedelta(days=1)).date()
    print('\tlast trade was {}'.format(pretty_datetime(tmp_date)))
    return tmp_date

# sqllite and MySQL
def update_price_data(symbol):
    print('\tupdate price data: {}'.format(symbol))
    symbol = symbol.upper()
    _date = last_price_upload(symbol)
    if _date is None:
        print('\t\tno price data for {}'.format(symbol))
        price_data = download_price_data(symbol)
        if price_data is None:
            return None
        else:
            upload_price_data(price_data)
            return price_data
    else:
        price_data = load_price_data(symbol)
        if last_trade_date() > _date:
            print('\t\tprice data for {} out of date ({})'.format(symbol, _date))
            # print('last_trade_date()={} > _date={}'.format(last_trade_date(), _date))
            _new_price_data = download_price_data(symbol, _date)
            if _new_price_data is None:
                return None
            # updated_prices = np.unique(np.concatenate((price_data, _new_price_data)))
            upload_price_data(_new_price_data)
            price_data = load_price_data(symbol)
            return price_data
        else:
            print('\t\tprice data for {} up to date'.format(symbol))
            return price_data

"""
TESTING
"""
def test_all():
    # symbols to be tested
    syms = ['AMG','MSFT','FOO']
    # dates to be tested
    friday = datetime.date(year=2015,month=1,day=2)
    test_days = [friday + datetime.timedelta(days=i) for i in range(4)]
    test_days.append(datetime.date.today())

    print('\nTESTING: last_trade_date')
    for test_day in test_days:
        print('testing: last_trade_date({})'.format(pretty_datetime(test_day)))
        last_trade_date(test_day)
    
    net_data = {}
    print('\nTESTING: download_price_data')
    for sym in syms:
        print('testing: download_price_data({})'.format(sym))
        net_data[sym] = download_price_data(sym)
    print(net_data[syms[0]][0])

    print('\nTESTING: upload_price_data')
    for sym in syms:
        if net_data[sym] is not None:
            print('testing: upload_price_data({})'.format(sym))
            upload_price_data(net_data[sym])

    db_data = {}
    print('\nTESTING: update_price_data')
    for sym in syms:
        # if data[sym] is not None:
        print('testing: update_price_data({})'.format(sym))
        db_data[sym] = update_price_data(sym)


if __name__ == '__main__':
    test_all()
    db_io.clean_up()

