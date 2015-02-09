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

    def __eq__(self, other):
        # return self.symbol == other.symbol and self.trans_date == other.trans_date and self.type == other.type and
        return str(self) == str(other)

    def __getitem__(self,arg):
        return self.__dict__[arg]

    @classmethod
    def names(cls):
        return cls.__col_names

class TimeSeriesTrans(list):
    """ time series container of Transaction object """
    
    def __init__(self, _list=None):
        if _list is None:
            _list = db_io.cur().execute(db_io.select_alltrans_stmt).fetchall()
            super(TimeSeriesTrans, self).__init__(map(lambda tran: Transaction(*tran),_list))
        else:
            super(TimeSeriesTrans, self).__init__(_list)
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
        tmp = TimeSeriesTrans(itertools.ifilter(lambda tran: tran[dargs.keys()[0]] == dargs.values()[0], self))
        return tmp

    def cat(self,trans):
        self.append(trans)
        self.sort()

    def holdings(self, dt=None, symbol=None):
        cash = 0
        qty = {}
        i = len(self) - 1
        if dt is None:
            dt = datetime.datetime.today().date()
        while self[i]['trans_date'] < dt and i > 0:
            # print(str(self[i])+' {: >7.2f}'.format(cash))
            if symbol is None or self[i]['symbol'] == symbol:
                cash += self[i]['amount']
                # print(trans_io.deposit_ty pe)
                if self[i]['type'] in ['BUY', 'SELL', trans_io.deposit_type]:
                    if self[i]['symbol'] not in qty:
                        qty[self[i]['symbol']] = self[i]['qty']
                    else:
                        qty[self[i]['symbol']] += self[i]['qty']
                if self[i]['symbol'] in qty and self[i]['symbol'] != trans_io.cash_symbol:
                    print(str(self[i])+' {: >9.2f} {: 5.0f}'.format(cash,qty[self[i]['symbol']]))
                else:
                    print(str(self[i])+' {: >9.2f}'.format(cash))
                    # print("*** qty ***")
            i-=1

    def book(self,dt=None,symbol=None):
        # TODO: calculate book value
        if dt is None:
            # return current book value
            dt = None # most last trade day
        if symbol is None:
            # return book value of entire portfolio
            symbol = [None,]
        return None

    def market(self,dt=None,symbol=None):
        # TODO: calculate market value
        if dt is None:
            # return current market value
            dt = None # most last trade day
        if symbol is None:
            # return market value of entire portfolio
            symbol = [None,]
        return None 

    def cash(self, dt=None):
        # TODO: calculate cash
        pass 



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


if __name__ == '__main__':
    ts = TimeSeriesTrans()
    # print(ts)
    # print(ts[0])
    # ts.ts_foo()
    # ts.holdings(symbol='RY')
    ts.holdings()
    mergers = list(ts.get(type='MERGER'))
    sym = 'SWY'
    # cm = ts.get(symbol=sym)
    # print(cm)
    # cm_h = Holding(sym, cm)
    # print(cm_h)
    # print(cm_h['qty'])

    # for merger in mergers:
        # print(merger)
        # print(merger.desc)
    



