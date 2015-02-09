__author__ = 'grahamcrowell'

import collections
import datetime
import itertools

import numpy as np

import db_io
import trans_io


"""
Abstract container for time series data
"""
Transaction = collections.namedtuple('Transaction', ['desc', 'symbol', 'trans_date', 'settle_date', 'acct_currency', 'type', 'qty', 'price_currency', 'price', 'amount'], verbose=False)


def namedtuple_load_trans_data(_symbol=None):
    if _symbol is None:
        trans_list = db_io.cur().execute(db_io.select_alltrans_stmt).fetchall()
        return list(map(lambda transaction: Transaction(*transaction), trans_list))
    # key,val = darg.items()[0]
    # sql_stmt = db_io.select_trans_stmt(key), (val,)
    # print(sql_stmt)
    # return db_io.cur().execute(unicode(sql_stmt)).fetchall()
    else:
        trans_list = db_io.cur().execute(db_io.select_trans_stmt, (_symbol,)).fetchall()
        return list(map(lambda transaction: Transaction(*transaction), trans_list))


# class Transaction(object):
# def __init__(self, _desc, _symbol, _trans_date, _settle_date, _acct_currency, _type, _qty, _price_currency, _price, _amount):
# self.desc = _desc
# self.symbol = _symbol
# self.trans_date = _trans_date
# self.settle_date = _settle_date
#         self.acct_currency = _acct_currency
#         self.type = _type
#         self.qty = _qty
#         self.price_currency = _price_currency
#         self.price = _price
#         self.amount = _amount

class Transaction:
    def __init__(self, desc, symbol, trans_date, settle_date, acct_currency, type, qty, price_currency, price, amount):
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

    def __lt__(self, other):
        if isinstance(other, datetime.datetime.date):
            return self.trans_date < other
        elif isinstance(other, Transaction):
            return self.trans_date < other.trans_date
        else:
            raise Exception('invalid comparison:\n {} < {} ({})'.format(self, other, type(other)))

    def __gt__(self, other):
        if isinstance(other, datetime.datetime.date):
            return self.trans_date > other
        elif isinstance(other, Transaction):
            return self.trans_date > other.trans_date
        else:
            raise Exception('invalid comparison:\n {} > {} ({})'.format(self, other, type(other)))

    def __str__(self):
        _format = '{trans_date}  {symbol: <7s}  {type: <8s}'
        if self.type in [trans_io.dividend_type,] or self.symbol in [trans_io.cash_symbol,]:
            # YYYY-MM-DD  SYM---  DIV (on ---# SHS)  $--,--#.##
            # _format += '{trans_date}  {symbol: <5s}  {type: <' + str(len(trans_io.dividend_type)) + 's} ' + '{: <15s}'.format('(on {qty:.0f} SHS)') + ' {: >15s}'.format('${amount: >.2f}')
            _format += '${amount: >8.2f}'
        elif self.type in ['BUY','SELL']:
            _format += '#{qty: >5.0f} x ${price: >8.2f} = ${amount: >10.2f}'
        elif self.type in [trans_io.deposit_type,]:
            _format += '#{qty: >5.0f}'
        return _format.format(**self.__dict__)

    def __repr__(self):
        _format = '{trans_date}  {symbol: <7s}  {type: >8s}'
        return '<'+_format.format(**self.__dict__)+'>'

    def __eq__(self, other):
        # return self.symbol == other.symbol and self.trans_date == other.trans_date and self.type == other.type and
        return str(self) == str(other)
    def __getitem__(self,arg):
        return self.__dict__[arg]

class Portfolio(list):
    """docstring for Portfolio"""
    def __init__(self, _list):
        super(Portfolio, self).__init__(map(lambda tran: Transaction(*tran),_list))
    def __getitem__(self,arg):
        return map(lambda tran: tran[arg], self)
    def get(self,**dargs):
        return itertools.ifilter(lambda tran: tran[dargs.keys()[0]] == dargs.values()[0], self)
    def foo(self,arg):
        if isinstance(arg,datetime.datetime):
            arg = arg.date()
        I = [i for i in range(len(self)) if self[i].trans_date < arg]
        print(I)
        dts = [dt for dt in self.dates if dt < arg]
        print(dts)
        trans = [trans for trans in self if trans.trans_date < arg]
        print(trans)
        print(len(trans))
        print(arg)





if __name__ == '__main__':
    print('TimeSeries.py')
    # ts = namedtuple_load_trans_data()
    # rim = list(itertools.ifilter(lambda transaction: transaction.symbol == 'RIM',ts))
    # print(rim)
    # print(len(rim),type(rim))

    trans_list = db_io.cur().execute(db_io.select_alltrans_stmt).fetchall()
    p = Portfolio(trans_list)
    p['symbol']
    x = p.get(symbol='XTR')
    print(list(x))
    x = p.get(type='DIV')
    print(list(x))
    for t in p:
        print(t)
    # dt = datetime.datetime(year=2009,month=3,day=15)
    # x = p[dt]
