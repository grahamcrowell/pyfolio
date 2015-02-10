
"""
datastructs.py
"""
def print_list(L):
    s = ''
    i = 0
    num_wid = str(len(str(len(L))))
    _format = '{: '+num_wid+'.0f} {}'
    for i in range(len(L)):
        print(_format.format(i,L[i]))


class Holding(object):
    __qty_io_names =['BUY', 'SELL', trans_io.deposit_type]
    def __init__(self, symbol, ts_tran):
        self.symbol = symbol
        self.ts = ts_tran

    def __getitem__(self, arg):
        if arg in self.__dict__.keys():
            return self.__dict__[arg]
        elif arg == 'qty':
            ios = map(lambda tran: tran['qty'],itertools.ifilter(lambda tran: tran.type in Holding.__qty_io_names, self.ts))
            tot = sum(ios)
            print(ios,tot)
            return tot
        elif arg == 'trans':
            return self.ts
        elif arg == 'book':
            pass

    def __str__(self):
        return '{} Holding'.format(self.symbol)

class Portfolio:
    """ snapshot of Portfolio for date dt """
    def __init__(self, dt, holdings):
        self.dt = dt
        self.holdings = holdings
    def __getitem__(self, arg):
        if arg in self.__dict__.keys():
            return self.__dict__[arg]
        # ????? cash 
        elif arg == trans_io.cash_symbol:
            pass    
            # ?????? ????????


def foo():
    trans_list = db_io.cur().execute(db_io.select_alltrans_stmt).fetchall()
    p = Portfolio(trans_list)
    p = Portfolio()
    p['symbol']
    x = p.get(symbol='XTR')
    print(list(x))
    x = p.get(type='DIV')
    print(list(x))
    for t in p:
        print(t)
    # dt = datetime.datetime(year=2009,month=3,day=15)
    # x = p[dt]

class TimeSeriesTrans(TimeSeries):
    """ time series container of Transaction object """
    
    def __init__(self, _list=None):
        if _list is None:
            _list = db_io.cur().execute(db_io.select_alltrans_stmt).fetchall()
            super(TimeSeriesTrans, self).__init__(map(lambda tran: Transaction(*tran),_list))
        else:
            super(TimeSeriesTrans, self).__init__(_list)

    # def __getitem__(self,arg):
    #     if isinstance(arg,str):
    #         if arg in TimeSeriesTrans.names():
    #             return map(lambda tran: tran[arg], self)
    #         else:
    #             raise NotImplimentedError()
    #     elif isinstance(arg,datetime.date):
    #         return itertools.ifilter(lambda tran: tran.dt < arg)
    #     elif isinstance(arg,datetime.datetime):
    #         return itertools.ifilter(lambda tran: tran.dt < arg.date())
    #     elif isinstance(arg,int):
    #         return super(TimeSeriesTrans, self).__getitem__(arg)

"""
price_io.py
"""



# class OHLC(object):
#     def __init__(self, _symbol, _date, _open, _high, _low, _close, _volume, _adj_close):
#         self.symbol = str(_symbol)
#         self.date = _date
#         self.open = _open
#         self.high = _high
#         self.low = _low
#         self.close = _close
#         self.volume = _volume
#         self.adj_close = _adj_close

#     def __repr__(self):
#         return "({symbol};{date};{open:.2f};{high:.2f};{low:.2f};{close:.2f};{volume};{adj_close:.2f})".format(**self.__dict__)

#     def __str__(self):
#         return "{symbol: <6s} {date} {open: >10.2f} {high: >10.2f} {low: >10.2f} {close: >10.2f} {volume: >10d} {adj_close: >10.2f}".format(**self.__dict__)

#     def get_tuple(self):
#         return self.symbol, self.date, self.open, self.high, self.low, self.close, self.volume, self.adj_close


# def adapt_ohlc(ohlc):
#     return ohlc.__repr__()


# def convert_ohlc(s):
#     s, d, o, h, l, c, v, a = map(float, s.split(";"))
#     return OHLC(s, d, o, h, l, c, v, a)


# sqlite3.register_adapter(OHLC, adapt_ohlc)
# sqlite3.register_converter("ohlc", convert_ohlc)



