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

def print_list(L):
    s = ''
    i = 0
    num_wid = str(len(str(len(L))))
    _format = '{: '+num_wid+'.0f} {}'
    for i in range(len(L)):
        print(_format.format(i,L[i]))


class Datum(object):
    """ base class for time series data point """
    
    def __init__(self, year, month, day, dpt):
        # datetime.date.__init__(year=year,month=month,day=day)
        self.dt = datetime.date(year=year,month=month,day=day)
        self.dpt = dpt


class Transaction(Datum):
    col_names = ['desc', 'symbol', 'trans_date', 'settle_date', 'acct_currency', 'type', 'qty', 'price_currency', 'price', 'amount']
    
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
        _format = '{symbol: <5s} {type: >8s} {trans_date}'
        return '<'+_format.format(**self.__dict__)+'>'

    def __eq__(self, other):
        # return self.symbol == other.symbol and self.trans_date == other.trans_date and self.type == other.type and
        return str(self) == str(other)

    def __getitem__(self,arg):
        return self.__dict__[arg]

    @classmethod
    def names(cls):
        return cls.col_names

class TimeSeriesTrans(list):
    """ time series container of Transaction object """
    
    def __init__(self, _list=None):
        if _list is None:
            _list = db_io.cur().execute(db_io.select_alltrans_stmt).fetchall()
        super(TimeSeriesTrans, self).__init__(map(lambda tran: Transaction(*tran),_list))
    
    def __getitem__(self,arg):
        if isinstance(arg,str):
            if arg in TimeSeriesTrans.names():
                return map(lambda tran: tran[arg], self)
            else:
                raise NotImplimentedError()
        elif isinstance(arg,datetime.date):
            return itertools.ifilter(lambda tran: tran.dt < arg)
        elif isinstance(arg,datetime.datetime):
            return itertools.ifilter(lambda tran: tran.dt < arg.date())
        elif isinstance(arg,int):
            return super(TimeSeriesTrans, self).__getitem__(arg)
            # return portfolio after initial deposits
            # return self.get(type='DEPOSIT')
            # [trans for trans in self if trans.trans_date < arg]
    
    def __str__(self):
        return '\n'.join(map(str,self))

    @classmethod
    def names(cls):
        return Transaction.col_names

    def get_col(self,arg):
        """ return column with name arg """
        return map(lambda tran: tran[arg], self)
    
    def get(self,**dargs):
        return itertools.ifilter(lambda tran: tran[dargs.keys()[0]] == dargs.values()[0], self)
    
    def ts_foo(self):
        i = len(self) - 1
        print(self[i]['type'])
        tmp_trans = []
        trans_dict = {}
        while self[i]['type'] == trans_io.deposit_type:
            tmp_trans.append(self[i])
            print(self[i])
            i-=1
        trans_dict[self[i+1].dt] = tmp_trans

        print(trans_dict)



    # def foo(self,arg):
    #     if isinstance(arg,datetime.datetime):
    #         arg = arg.date()
    #     I = [i for i in range(len(self)) if self[i].trans_date < arg]
    #     print(I)
    #     dts = [dt for dt in self.dates if dt < arg]
    #     print(dts)
    #     trans = [trans for trans in self if trans.trans_date < arg]
    #     print(trans)
    #     print(len(trans))
    #     print(arg)




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


if __name__ == '__main__':
    ts = TimeSeriesTrans()
    print(ts)
    # print(ts[0])
    ts.ts_foo()
    mergers = list(p.get(type='MERGER'))
    # for merger in mergers:
        # print(merger)
        # print(merger.desc)
    



