__author__ = 'grahamcrowell'

import datetime
import itertools
import collections
from operator import itemgetter
dict = collections.OrderedDict
import db_io
import trans_io
import price_io


"""
Containers for time series data
"""


class Datum(object):
    """ base class for time series data point """
    
    def __init__(self, year, month, day, dpt):
        # datetime.date.__init__(year=year,month=month,day=day)
        self.dt = datetime.date(year=year,month=month,day=day)
        self.dpt = dpt

    def __lt__(self, other):
        if isinstance(other, datetime.datetime.date):
            return self.trans_date < other
        elif isinstance(other, Datum):
            return self.trans_date < other.trans_date
        else:
            raise Exception('invalid comparison:\n {} < {} ({})'.format(self, other, type(other)))

    def __gt__(self, other):
        if isinstance(other, datetime.datetime.date):
            return self.trans_date > other
        elif isinstance(other, Datum):
            return self.trans_date > other.trans_date
        else:
            raise Exception('invalid comparison:\n {} > {} ({})'.format(self, other, type(other)))

    def __eq__(self, other):
        # return self.symbol == other.symbol and self.trans_date == other.trans_date and self.type == other.type and
        return str(self) == str(other)

    def __getitem__(self,arg):
        return self.__dict__[arg]

    @classmethod
    def names(cls):
        return cls.__col_names


class TimeSeries(list):
    """ base class for time series data container """
    def __init__(self, _list=None):
        super(TimeSeries, self).__init__(_list)

    def __str__(self):
        return '\n'.join(map(str,self))

    def __getitem__(self,arg):
        if isinstance(arg,str):
            if arg in self.__class__.names():
                return map(lambda tran: tran[arg], self)
        elif isinstance(arg,int):
            return super(TimeSeries, self).__getitem__(arg)
        else:
            raise NotImplementedError()

    def get_col(self,arg):
        """ return column with name arg """
        return map(lambda element: element[arg], self)

    @classmethod
    def names(cls):
        raise NotImplementedError()


class OHLC(Datum):
    __col_names = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'adj_close']
   
    def __init__(self, symbol, date, open, high, low, close, volume, adj_close):
        
        Datum.__init__(self,year=date.year,month=date.month,day=date.day, dpt=close)
        self.symbol = str(symbol)
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.adj_close = adj_close

    def __repr__(self):
        return '<'+'{symbol:_<7s}_{date}_{close:_>5.2f}_${adj_close: >5.2f}'.format(**self.__dict__) +'>'

    def __str__(self):
        return "{symbol: <7s} {date} {open: >10.2f} {high: >10.2f} {low: >10.2f} {close: >10.2f} {volume: >10d} {adj_close: >10.2f}".format(**self.__dict__)

    # def get_tuple(self):
        return self.symbol, self.date, self.open, self.high, self.low, self.close, self.volume, self.adj_close


class TimeSeriesPrices(TimeSeries):

    def __init__(self, arg=None):
        if arg is None:
            raise Exception()
        elif isinstance(arg,str):
            _list = price_io.load_price_data(arg)
            super(TimeSeriesPrices, self).__init__(map(lambda element: OHLC(*element),_list))
        elif isinstance(arg,TimeSeriesPrices):
            super(TimeSeriesPrices, self).__init__(arg)
        else:
            raise Exception()

    @classmethod
    def get_prices(cls,symbol):
        price_data = price_io.update_price_data(symbol)
        return cls(price_data)


class Transaction(Datum):
    __col_names = ['desc', 'symbol', 'trans_date', 'settle_date', 'acct_currency', 'type', 'qty', 'price_currency', 'price', 'amount']
    
    def __init__(self, desc, symbol, trans_date, settle_date, acct_currency, type, qty, price_currency, price, amount):
        Datum.__init__(self,year=trans_date.year,month=trans_date.month,day=trans_date.day, dpt=amount)
        self.desc = str(desc)
        self.symbol = str(symbol)
        self.trans_date = trans_date
        self.settle_date = settle_date
        self.acct_currency = str(acct_currency)
        self.type = str(type)
        self.qty = float(qty)
        self.price_currency = str(price_currency)
        self.price = float(price)
        self.amount = float(amount)

    def __str__(self):
        _format = '{trans_date}  {symbol: <7s}  {type: <8s}'
        if self.type in [trans_io.dividend_type,] or self.symbol in [trans_io.cash_symbol,]:
            # YYYY-MM-DD  SYM---  DIV (on ---# SHS)  $--,--#.##
            # _format += '{trans_date}  {symbol: <5s}  {type: <' + str(len(trans_io.dividend_type)) + 's} ' + '{: <15s}'.format('(on {qty:.0f} SHS)') + ' {: >15s}'.format('${amount: >.2f}')
            _format += ' '*21 + '${amount: >10.2f}'
        elif self.type in ['BUY','SELL']:
            _format += '#{qty: >5.0f} x ${price: >8.2f} = ${amount: >10.2f}'
        elif self.type in [trans_io.deposit_type,]:
            _format += '#{qty: >5.0f}' + ' '*26
        else:
            _format += ' '*32
        return _format.format(**self.__dict__)

    def __repr__(self):
        _format = '{symbol} {type} {trans_date}'
        return '<'+_format.format(**self.__dict__)+'>'

    @classmethod
    def names(cls):
        return cls.__col_names

class TimeSeriesTrans(TimeSeries):
    
    def __init__(self, _list=None):
        if _list is None:
            _list = trans_io.load_trans_data()
            print(_list)
            super(TimeSeriesTrans, self).__init__(map(lambda tran: Transaction(*tran),_list))
        else:
            super(TimeSeriesTrans, self).__init__(_list)
        # self.holdings_ts = {}

    @classmethod
    def names(cls):
        return Transaction.names()
    
    def get(self,**dargs):
        tmp = TimeSeriesTrans(itertools.ifilter(lambda tran: tran[dargs.keys()[0]] == dargs.values()[0], self))
        return tmp

    def init_time_series(self):
        current_cash = 0
        qty_held = dict()
        time_series = dict()
        i = len(self) - 1
        curr_date = self[i]['trans_date'] + datetime.timedelta(days=1)
        while i > 0:

            if self[i]['trans_date'] > curr_date:
                # transactions on curr_date processed
                # add all non-zero qty holdings for date curr_date
                holdings = dict({trans_io.cash_symbol:current_cash})
                for sym, qty in qty_held.items():
                    if int(qty) != 0:
                        # todo: look up market price
                        holdings[sym] = qty
                time_series[curr_date] = holdings
                curr_date = self[i]['trans_date']

            # process transaction
            current_cash += self[i]['amount']
            # transaction is a trade or initial deposit
            if self[i]['type'] in ['BUY', 'SELL', trans_io.deposit_type]:
                # add symbol to 
                if self[i]['symbol'] not in qty_held:
                    qty_held[self[i]['symbol']] = self[i]['qty']
                else:
                    qty_held[self[i]['symbol']] += self[i]['qty']
            # if self[i]['symbol'] in qty_held and self[i]['symbol'] != trans_io.current_cash_symbol:
            #     print(str(self[i])+' {: >9.2f} {: 5.0f}'.format(current_cash,qty_held[self[i]['symbol']]))
            # else:
            #     # current_cash only transaction
            #     print(str(self[i])+' {: >9.2f}'.format(current_cash))
            i-=1
        return time_series

    def book(self,dt=None,symbol=None):
        # TODO: calculate book value
        pass

    def market(self,dt=None,symbol=None):
        # TODO: calculate market value
        pass

    def cash(self, dt=None):
        # TODO: calculate cash
        pass 



def trans_foo():
    ts = TimeSeriesTrans()
    # print(ts['desc'])
    # print(ts[0])
    # print(ts)
    # ts.holdings(symbol='RY')
    ts.init_time_series()
    # print(ts.init_time_series_ts)
    for dt,holdings in ts.init_time_series().items():
        print('{}'.format(dt))
        for sym,qty in holdings.items():
            print('\t{: <7s} {: >5.0f}'.format(sym,qty))


if __name__ == '__main__':
    trans_foo()
    # ts = TimeSeriesTrans()
    # s = 'MSFT'
    # ts = TimeSeriesPrices(s)
    # print(ts)
    pass





