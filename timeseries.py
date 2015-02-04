__author__ = 'grahamcrowell'

import collections
import datetime

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
        _format = '{trans_date}  {symbol: <5s}  {type: <12s}'
        if self.type == trans_io.dividend_type:
            # YYYY-MM-DD  SYM---  DIV (on ---# SHS)  $--,--#.##
            _format = '{trans_date}  {symbol: <5s}  {type: <' + str(len(trans_io.dividend_type)) + 's} ' + '{: <15s}'.format('(on {qty:.0f} SHS)') + ' {: >15s}'.format('${amount: >.2f}')
            print(_format)
        elif self.symbol in [trans_io.]
            return _format.format(**(self.__dict__))
            # if self.type == trans_io.

    def __eq__(self, other):
        # return self.symbol == other.symbol and self.trans_date == other.trans_date and self.type == other.type and
        return str(self) == str(other)


class TimeSeries(object):
    def __init__(self):
        pass


if __name__ == '__main__':
    print('TimeSeries.py')
    # ts = namedtuple_load_trans_data()
    # rim = list(itertools.ifilter(lambda transaction: transaction.symbol == 'RIM',ts))
    # print(rim)
    # print(len(rim),type(rim))

    trans_list = db_io.cur().execute(db_io.select_alltrans_stmt).fetchall()
    # print(trans_list)
    t = Transaction(*trans_list[0])
    for t in trans_list[0:10]:
        print(t)

    print(t)