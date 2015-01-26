__author__ = 'grahamcrowell'

# todo is numpy required/beneficial?

import datetime
import itertools
import os

import numpy as np

import db_io


rrsp_csv_name = "transactionHistory_all_all_all.csv"
print('csv transaction history file:\n\t{}\n'.format(rrsp_csv_name))
rrsp_csv_file = os.path.join(db_io.data_dir_path, rrsp_csv_name)

max_rrsp_transaction_cnt = 1000
db_tran_dtype = np.dtype(
    [('desc', np.str_, 100), ('symbol', np.str_, 10), ('trans_date', 'datetime64[D]'), ('settle_date', np.datetime64), ('acct_currency', np.str_, 6), ('type', np.str_, 15), ('qty', np.float32),
     ('price_currency', np.str_, 6), ('price', np.float32), ('amount', np.float32)])
rrsp_tran_dtype = np.dtype(
    [('desc', np.str_, 100), ('symbol', np.str_, 10), ('trans_date', np.str_, 10), ('settle_date', np.str_, 10), ('acct_currency', np.str_, 6), ('type', np.str_, 15), ('qty', np.float32),
     ('price_currency', np.str_, 6), ('price', np.float32), ('amount', np.float32)])
# rrsp transaction CSV header
transaction_header = '''Description,Symbol,Transaction Date,Settlement Date,Account Currency,Type,Quantity,Currency of Price,Price,Settlement Amount'''

# 02-Jul-2014
rrsp_date_format = "%d-%b-%Y"
# 2014-07-02
iso_date_format = "%Y-%m-%d"


def pprint(arr):
    if arr.dtype == db_tran_dtype:
        out = ''
        for x in arr:
            out += '{: <6s} {: 10s}\n'.format(x['symbol'], str(x['trans_date']))
        return out
    else:
        np.set_string_function(None)
        s = arr.__repr__()
        np.set_string_function(pprint, repr=False)
        return s
        # return 'id like to say hello'


# np.set_string_function(pprint)
# np.set_string_function(lambda x: 'random', repr=False)
# np.set_string_function(pprint, repr=False)

# todo rrsp_tran_dtype fancy to string
def pprint(_arr):
    if _arr.dtype is not rrsp_tran_dtype:
        return repr(_arr)
    else:
        return repr(_arr)


# np.set_string_function(pprint, repr=False)

cash_symbol = '<cash>'
fee_symbol = '<fee>'


def extract_date_rrsp(_dt_str):
    # 02-Jul-2014
    py_dt = datetime.datetime.strptime(_dt_str, rrsp_date_format).date()
    np_dt = np.datetime64(py_dt.isoformat())
    return np_dt


def parse_rrsp_line(_line):
    tkns = _line.split(',')
    tkns[2] = extract_date_rrsp(tkns[2])
    tkns[3] = extract_date_rrsp(tkns[3])
    tkns[6] = float(tkns[6])
    return tkns


def load_rrsp_csv_trans(_full_path=rrsp_csv_file):
    # todo convert to functional style
    arr = np.zeros(max_rrsp_transaction_cnt, dtype=rrsp_tran_dtype)
    _file = open(_full_path)
    _file.readline()
    i = 0
    for line in _file:
        # print(line)
        rrsp_line = tuple(parse_rrsp_line(line))
        arr[i] = rrsp_line[0:len(rrsp_line) - 1]
        i += 1
    _file.close()
    return arr[0:i]


def bool_or(_arr, _args=None, **_dargs):
    """
    filter ndarray 
    :param _arr: ndarray
    :param args: dict of {key:[value,...]} pairs
    :param dargs: key=[value,...] parameters
    :return: array for boolean indexing
    """
    if _args is None:
        print('no args parameter given')
        _args = {}
    _args.update(_dargs)
    i = np.array(len(_arr), dtype=np.bool)
    i = False
    for key, val in _args.items():
        if isinstance(val, list) or isinstance(val, tuple):
            for sub_val in val:
                j = (_arr[key] == sub_val)
                i = (i + j)
        else:
            print('not list')
            i = i + (_arr[key] == val)
    return i


def voodoo_fill(_arr, sub_len=3):
    """
    fills in missing symbol by matching first sub_len words in desc to rows where symbol is known
    :param _arr: ndarray of dtype = rrsp_tran_dtype
    :param _type:
    :param sub_len: number of words to match in desc column
    :return:
    """
    cnt = np.sum((_arr['symbol'] == ' '))
    print('\tvoodoo symbol fill ({} missing, sub_len={})'.format(cnt, sub_len))
    # get all complete rows
    SYMS = _arr[_arr['symbol'] != ' ']
    # make array of desc and symbol of comple rows
    LOOKUP = np.column_stack((SYMS['desc'], SYMS['symbol']))
    # extract first few words from desc of each row of type
    # type = 'CASH DIV'
    sub_names = list(map(lambda row: ' '.join(row[0].split(' ')[0:sub_len]), _arr))
    # get list of rows with substring in LOOKUP
    sub_syms = list(map(lambda sub: list(itertools.ifilter(lambda desc: sub in desc[0], LOOKUP)), sub_names))
    # define function to extract symbol from a list in sub_syms
    def abc(x):
        if len(x) == 0:
            return ' '
        else:
            return x[0][1]

    # apply above function to extract symbol from match LOOKUP which are stored in sub_syms
    # assign these symbols to fill missing symbol rows
    _arr['symbol'] = list(map(abc, sub_syms))

    cnt2 = np.sum(_arr['symbol'] == ' ')
    print('\t\t{} symbols added ({} still empty)'.format(cnt - cnt2, cnt2))
    return _arr


def ad_hoc_fill(_arr):
    cnt = np.sum((_arr['symbol'] == ' '))
    print('\tad hoc symbol fill ({} missing)'.format(cnt))
    _arr['symbol'][np.where(_arr['type'] == 'Funds Transfer')] = cash_symbol
    _arr['symbol'][np.where(_arr['type'] == 'CASHINLIEU')] = cash_symbol
    _arr['symbol'][np.where(_arr['type'] == 'HST')] = fee_symbol
    _arr['symbol'][np.where(_arr['type'] == 'GST')] = fee_symbol
    _arr['symbol'][np.where(_arr['type'] == 'TGL')] = fee_symbol
    _arr['symbol'][np.where(_arr['desc'] == 'BLACKBERRY LTD COM RESULT OF NAME CHANGE      ')] = 'RIM'
    _arr['symbol'][np.where(_arr['desc'] == 'ISHARES NATURAL GAS COMMODITY INDEX FD COM UNIT RESULT OF NAME CHANGE      ')] = 'GAS'
    _arr['symbol'][_arr['desc'] == 'CASH TRANSFER      '] = cash_symbol
    cnt2 = np.sum(_arr['symbol'] == ' ')
    print('\t\t{} symbols added ({} still empty)'.format(cnt - cnt2, cnt2))


def symbol_fill(_arr):
    # todo handle remaining empty symbol rows
    print('symbol fill')
    ad_hoc_fill(_arr)
    voodoo_fill(_arr, 3)
    voodoo_fill(_arr, 2)
    cnt2 = np.sum(_arr['symbol'] == ' ')
    print('{} symbols still empty'.format(cnt2))


def upload_trans_data(_data):
    '''
    INSERT OR IGNORE upload to database trans table
    :param _data: list of tuples which represent rows
    :return: None
    '''
    print('\tuploading {} trans data'.format(len(_data)))
    db_io.cur().executemany(db_io.upload_trans_stmt, _data)


def rebuild_trans_table():
    print('* * * remaking trans table')
    user = raw_input('\tall trans data will be lost.\n\t\tENTER T to confirm')
    if user.upper() == 'T':
        db_io.cur().execute(db_io.delete_trans_stmt)
        db_io.con().commit()
        print('\ttable deleted')
        db_io.init_db()
        ts = load_rrsp_csv_trans()
        symbol_fill(ts)
        data = ts.tolist()
        upload_trans_data(data)
        print('\ttable rebuilt and re-uploaded')
    else:
        print('\ttable not delete, left as is')


def load_trans_data(**darg):
    key, val = darg.items()[0]
    sql_stmt = db_io.select_trans_stmt(key), (val,)
    print(sql_stmt)
    return db_io.cur().execute(unicode(sql_stmt)).fetchall()


def load_trans_data(_symbol=None):
    if _symbol is None:
        trans_list = db_io.cur().execute(db_io.select_alltrans_stmt).fetchall()
        return np.array(trans_list, dtype=db_tran_dtype)
    # key,val = darg.items()[0]
    # sql_stmt = db_io.select_trans_stmt(key), (val,)
    # print(sql_stmt)
    # return db_io.cur().execute(unicode(sql_stmt)).fetchall()
    else:
        return db_io.cur().execute(db_io.select_trans_stmt, (_symbol,)).fetchall()


def dtype2csv(row):
    # ISHARES 1 TO 3 YEAR TREASURY BOND ETF DIST      ON       4 SHS REC 12/29/14 PAY 12/31/14      , ,31-Dec-2014,31-Dec-2014,CAD,CASH DIV,0.00,CAD,0.000,0.15,
    tkns = row.split(',')
    tran_date_str = datetime.datetime.strptime(tkns[2], iso_date_format).date().strftime(rrsp_date_format)
    sett_date_str = datetime.datetime.strptime(tkns[3], iso_date_format).date().strftime(rrsp_date_format)
    line = '{},{},{},{},{},{},{:.2f},{},{:.3f},{:.2f}'.format(tkns[0], tkns[1], tran_date_str, sett_date_str, tkns[4], tkns[5], float(tkns[6]), tkns[7], float(tkns[8]), float(tkns[9]))
    return line


def save_rrsp_csv_trans(_arr):
    out_rrsp_csv_file = os.path.join(db_io.data_dir_path, 'TRANS_CSV_DATA-{}.csv'.format(datetime.date.today().strftime(iso_date_format)))
    with open(out_rrsp_csv_file, 'w') as csv_file:
        csv_file.write(transaction_header + '\n')
        for tran in _arr:
            inline = '{},{},{},{},{},{},{},{},{},{}'.format(*tran.tolist())
            outline = dtype2csv(inline) + '\n'
            csv_file.write(outline)


if __name__ == '__main__':
    # ts = load_rrsp_csv_trans()
    # symbol_fill(ts)
    # save_rrsp_csv_trans(ts)

    # ts = load_trans_data()
    # print(str(ts['trans_date'][0]))
    # print(str(ts))
    # print(ts.__str__())
    # print(type(ts))
    # x = np.arange(10)
    # print(x)
    # print(np.get_printoptions())
    # rebuild_trans_table()
    # db_io.dump_db()
    # db_io.clean_up()
    pass