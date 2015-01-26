__author__ = 'grahamcrowell'

import datetime
import urllib2
import sqlite3
import db_io


""" sqlite3  DATABASE """


class OHLC(object):
    def __init__(self, _symbol, _date, _open, _high, _low, _close, _volume, _adj_close):
        self.symbol = _symbol
        self.date = _date
        self.open = _open
        self.high = _high
        self.low = _low
        self.close = _close
        self.volume = _volume
        self.adj_close = _adj_close

    def __repr__(self):
        return "({symbol};{date};{open:.2f};{high:.2f};{low:.2f};{close:.2f};{volume};{adj_close:.2f})".format(**self.__dict__)

    def __str__(self):
        return "{symbol: <6s} {date} {open: >10.2f} {high: >10.2f} {low: >10.2f} {close: >10.2f} {volume: >10d} {adj_close: >10.2f}".format(**self.__dict__)

    def get_tuple(self):
        return self.symbol, self.date, self.open, self.high, self.low, self.close, self.volume, self.adj_close


def adapt_ohlc(ohlc):
    return ohlc.__repr__()


def convert_ohlc(s):
    s, d, o, h, l, c, v, a = map(float, s.split(";"))
    return OHLC(s, d, o, h, l, c, v, a)


sqlite3.register_adapter(OHLC, adapt_ohlc)
sqlite3.register_converter("ohlc", convert_ohlc)


def upload_price_data(_data):
    '''
    INSERT OR IGNORE upload to database price table
    :param _data: list of tuples which represent rows
    :return: None
    '''
    print('\tuploading {} price data'.format(len(_data)))
    db_io.cur().executemany(db_io.upload_price_stmt, _data)


def price_data_cur(_symbol):
    return db_io.con().cursor().execute(db_io.select_price_stmt, (_symbol,))


def load_price_data(_symbol):
    return db_io.cur().execute(db_io.select_price_stmt, (_symbol,)).fetchall()


def last_price_upload(_symbol):
    dt_str = db_io.cur().execute(db_io.lastup_price_stmt, (_symbol,)).fetchone()[0]
    if dt_str is None:
        return None
    else:
        _format = "%Y-%m-%d"
        return datetime.datetime.strptime(dt_str, _format).date()


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
            print('\tno price data of {} available for after {} retrying...'.format(_symbol, _date))
            return download_price_data(_symbol)
        else:
            print('\tERROR: no price data available for {}'.format(_symbol))
            return None
    _data = _request.read()
    lines = _data.split('\n')[1:-1]
    _data = list(map(lambda line: parse_price_line(_symbol, line), lines))
    # arr = np.array(_data, dtype=price_dtype)
    return _data


def last_trade_date(_datetime=None):
    # print('last_trade_date(_datetime={})'.format(_datetime))
    if _datetime is None:
        _datetime = datetime.datetime.now()
    elif isinstance(_datetime, datetime.date):
        _datetime = datetime.datetime(year=_datetime.year, month=_datetime.month, day=_datetime.day, hour=16)
    print('\tcurrent datetime: {}'.format(_datetime.strftime('%A %B %d %H:%M')))
    # today is a weekday
    if _datetime.weekday() < 5:
        # markets are closed
        if _datetime.hour >= 16:
            return _datetime.date()
        # elif _datetime.hour <=
        # today is monday
        elif _datetime.weekday() == 0:
            return (_datetime - datetime.timedelta(days=3)).date()
    # today is sunday
    elif _datetime.weekday() == 6:
        print('sunday')
        return (_datetime - datetime.timedelta(days=2)).date()
    # today is saturday, or markets are open
    # print('last full trade day was yesterday')
    return (_datetime - datetime.timedelta(days=1)).date()


def update_price_data(symbol):
    print('update price data: {}'.format(symbol))
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
# delete and start fresh database
#

# create/connect database
# init_db()

""" save to custom OHLC """
# s = u'MSFT'
# d = datetime.date(year=2013, month=2, day=21)
# o = 12.
# h = 23.43
# l = 10.2
# c = 23.2
# v = 123232
# a = .02
#
# ohlc1 = OHLC(s, d, o, h, l, c, v, a)
# d = datetime.date(year=2013, month=2, day=22)
# ohlc2 = OHLC(s, d, o, h, l, c, v, a)
# ohlc_list = [ohlc1.get_tuple(), ohlc2.get_tuple()]
# print(ohlc_list)
# _cur.executemany(insert_price_stmt, ohlc_list)

""" load existing data """
# s = 'MSFT'
# data = load_price_data(s)
# print(len(data))
# for p in data:
# print(p)

""" download and save all price data """
# s = 'MSFT'
# ohlc_list = download_price_data(s)
# data = ohlc_list
# for p in data:
# print(p)
# upload_price_data(data)

""" get last update """

if __name__ == '__main__':
    s = 'MSFT'
    update_price_data(s)
    db_io.remake_db()
    update_price_data(s)
    db_io.dump_db()
    db_io.clean_up()
