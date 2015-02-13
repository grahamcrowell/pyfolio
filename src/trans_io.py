__author__ = 'grahamcrowell'

# TODO: consolidate transaction types

import datetime
import itertools
import os

import numpy as np

import db_io

user = 'T'

rrsp_csv_name = "transactionHistory_all_all_all.csv"
print('csv transaction history file:\n\t{}\n'.format(rrsp_csv_name))
rrsp_csv_file = os.path.join(db_io.data_dir_path, rrsp_csv_name)
assert os.path.isfile(rrsp_csv_file)

db_tran_dtype = np.dtype(
    [('note', np.str_, 100), ('symbol', np.str_, 10), ('trans_date', 'datetime64[D]'), ('settle_date', 'datetime64[D]'), ('acct_currency', np.str_, 6), ('type', np.str_, 15), ('qty', np.float32), ('price_currency', np.str_, 6), ('price', np.float32), ('amount', np.float32)])
rrsp_tran_dtype = np.dtype(
    [('desc', np.str_, 100), ('symbol', np.str_, 10), ('trans_date', np.str_, 10), ('settle_date', np.str_, 10), ('acct_currency', np.str_, 6), ('type', np.str_, 15), ('qty', np.float32),
     ('price_currency', np.str_, 6), ('price', np.float32), ('amount', np.float32)])
# rrsp transaction CSV header
transaction_header = '''Description,Symbol,Transaction Date,Settlement Date,Account Currency,Type,Quantity,Currency of Price,Price,Settlement Amount'''

# 02-Jul-2014
rrsp_date_format = "%d-%b-%Y"
# 2014-07-02
iso_date_format = "%Y-%m-%d"
iso_datetime_format = "%Y-%m-%d %H:%M:%S"

def pprint(_arr):
    if _arr.dtype is not rrsp_tran_dtype:
        return repr(_arr)
    else:
        return repr(_arr)



cash_symbol = '$cash'
fee_symbol = '$fee'
wash_symbol = '$wash'
# ['BUY' 'CASH DIV' 'CASHINLIEU' 'CNCL SELL' 'Dividend' 'EXCHADJ' 'Exchange', 'FEE' 'Funds Transfer' 'GST' 'HST' 'MERGER' 'NAMECHG' 'REVERSE' 'SELL', 'STOCKDIV' 'TGL' 'TRANSFER' 'Transfer In' 'Trust Dividend']
# CASH DIV = Dividend = STOCKDIV = Trust Dividend
dividend_type = 'DIV'
wash_type = 'WASH'
deposit_type = 'INI'
cancel_sell_type = '*SELL'
merger_type = 'MRG'

def extract_date_rrsp(_dt_str):
    # 02-Jul-2014
    py_dt = datetime.datetime.strptime(_dt_str, rrsp_date_format).date()
    iso_dt = py_dt.isoformat()
    # np_dt = np.datetime64(py_dt.isoformat())
    return iso_dt

def parse_rrsp_line(_line):
    # print(_line)
    tkns = _line.strip().split(',')[0:-1]
    tkns[2] = extract_date_rrsp(tkns[2])
    tkns[3] = extract_date_rrsp(tkns[3])
    tkns[6] = float(tkns[6])
    # print(tkns)
    return tuple(tkns)

def load_rrsp_csv_trans(_full_path=rrsp_csv_file):
    # todo convert to functional style
    # sqllite / numpy 1.6
    arr = np.zeros(max_rrsp_transaction_cnt, dtype=rrsp_tran_dtype)
    # MySQL / numpy 1.7
    arr = np.zeros(max_rrsp_transaction_cnt, dtype=db_tran_dtype)
    _file = open(_full_path)
    _file.readline()
    i = 0
    for line in _file:
        # print(line)
        rrsp_line = tuple(parse_rrsp_line(line))
        # print(rrsp_line)
        arr[i] = rrsp_line[0:len(rrsp_line) - 1]
        i += 1
    _file.close()
    return arr[0:i]

def load_rrsp_csv_trans(_full_path=rrsp_csv_file):
    print('loading rrsp trans data from:\n\t{}'.format(_full_path))
    with open(_full_path,'r') as rrsp_csv:
        trans = list(map(lambda line: parse_rrsp_line(line), rrsp_csv.readlines()[1:]))
    arr = np.array(trans, dtype=db_tran_dtype)
    print('\t{} transactions loaded'.format(len(arr)))
    return arr
    




def voodoo_fill(_arr, sub_len=3):
    """
    fills in missing symbol by matching first sub_len words in note to rows where symbol is known
    :param _arr: ndarray of dtype = rrsp_tran_dtype
    :param _type:
    :param sub_len: number of words to match in note column
    :return:
    """
    # ***
    # note starting with "AS OF 12/15/09" are WRONGLY given symbol GAS
    # this is correctly manually in ad_hoc_fill
    # ***

    cnt = np.sum((_arr['symbol'] == ' '))
    print('\tvoodoo symbol fill ({} missing, sub_len={})'.format(cnt, sub_len))
    # get all complete rows
    SYMS = _arr[_arr['symbol'] != ' ']
    # make array of note and symbol of comple rows
    LOOKUP = np.column_stack((SYMS['note'], SYMS['symbol']))
    # extract first few words from note of each row of type
    # type = 'CASH DIV'
    sub_names = list(map(lambda row: ' '.join(row[0].split(' ')[0:sub_len]), _arr))
    # get list of rows with substring in LOOKUP
    sub_syms = list(map(lambda sub: list(itertools.ifilter(lambda note: sub in note[0], LOOKUP)), sub_names))
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
    # handle name changes
    _arr['symbol'][np.where(_arr['note'] == 'BLACKBERRY LTD COM RESULT OF NAME CHANGE      ')] = 'BB.TO'
    _arr['symbol'][np.where(_arr['symbol'] == 'RIM')] = 'BB.TO'
    _arr['symbol'][np.where(_arr['note'] == 'ISHARES NATURAL GAS COMMODITY INDEX FD COM UNIT RESULT OF NAME CHANGE      ')] = 'GAS'
    _arr['symbol'][np.where(_arr['note'] == 'ISHARES BROAD COMMODITY INDEX FD CAD HEDGED COM UNIT AS OF 11/30/12 SHRS RECEIVED THRU MERGER      ')] = 'CBR'
    _arr['symbol'][np.where(_arr['note'] == 'ISHARES S&P/TSX INCOME TRUST INDEX FUND RESULT OF NAME CHANGE      ')] = 'XTR'
    _arr['symbol'][np.where(_arr['note'] == 'ISHARES CDN S&P/TSX INCOME TRUST INDEX FUND DIST      ON    1000 SHS REC 03/30/10 PAY 03/31/10      ')] = 'XTR'
    # delete meaningless transactions
    _arr = np.delete(_arr, np.where(_arr['type'] == 'ADJUSTMENT'))
    _arr = np.delete(_arr, np.where(_arr['type'] == 'Exchange'))
    # handle partial dividends resulting from internal iTrade account change over
    _arr['symbol'][np.where(_arr['note'] == 'AS OF 12/15/09 DIV    COCA COLA CO      ')] = 'KO'
    _arr['symbol'][np.where(_arr['note'] == 'AS OF 12/15/09 DIV    TIM HORTONS INC      ')] = 'THI'
    _arr['symbol'][np.where(_arr['note'] == 'AS OF 12/15/09 DIV    TESORO CORPORATION      ')] = 'TSO'
    _arr['symbol'][np.where(_arr['note'] == 'AS OF 12/11/09 DIV    SPEEDWAY MOTOR SPORT      ')] = 'TRK'
    _arr['symbol'][np.where(_arr['note'] == 'AS OF 12/10/09 DIV    ELI LILLY & CO      ')] = 'LLY'
    _arr['symbol'][np.where(_arr['note'] == 'AS OF 12/07/09 DIV    ISHARES BARC 1-3 YR      ')] = 'SHY'
    _arr['symbol'][np.where(_arr['note'] == 'AS OF 01/04/10 DIV    HUSKY ENERGY INC      ')] = 'HSE'
    _arr['type'][np.where(_arr['type'] == 'TRANSFER')] = dividend_type

    # set symbol = cash_symbol for all cash transactions
    _arr['symbol'][np.where(_arr['type'] == 'HST')] = cash_symbol
    _arr['symbol'][np.where(_arr['type'] == 'GST')] = cash_symbol
    _arr['symbol'][_arr['note'] == 'CASH TRANSFER      '] = cash_symbol
    # consolidate dividend transactions
    # CASH DIV = Dividend = STOCKDIV = Trust Dividend => dividend_type
    _arr['type'][np.where(_arr['type'] == 'CASH DIV')] = dividend_type
    _arr['type'][np.where(_arr['type'] == 'Dividend')] = dividend_type
    _arr['type'][np.where(_arr['type'] == 'STOCKDIV')] = dividend_type
    _arr['type'][np.where(_arr['type'] == 'Trust Dividend')] = dividend_type

    # todo handle wash trade transactions
    # Funds Transfer = TGL => wash_type
    _arr['symbol'][np.where(_arr['type'] == 'Funds Transfer')] = cash_symbol
    _arr['type'][np.where(_arr['type'] == 'Funds Transfer')] = wash_type
    _arr['symbol'][np.where(_arr['type'] == 'TGL')] = cash_symbol
    _arr['type'][np.where(_arr['type'] == 'TGL')] = wash_type

    _arr['type'][np.where(_arr['type'] == 'Transfer In')] = deposit_type
    _arr['qty'][np.where(_arr['type'] == deposit_type)] = np.abs(_arr['qty'][np.where(_arr['type'] == deposit_type)])

    # todo handle GAS -> CBL merger (CASHINLIEU)
    _arr['symbol'][np.where(_arr['type'] == 'CASHINLIEU')] = cash_symbol
    _arr['type'][np.where(_arr['type'] == 'CASHINLIEU')] = merger_type

    _arr['type'][np.where(_arr['type'] == 'CNCL SELL')] = cancel_sell_type

    # todo handle splits:  type=EXCHADJ (one to many) or type=REVERSE (many to one)

    # wash trades and mergers consist of multiple transactions
    # todo create model/object for multi-transaction events that can be extended to option straegies (eg convered calls etc)

    cnt2 = np.sum(_arr['symbol'] == ' ')
    print('\t\t{} symbols added ({} still empty)'.format(cnt - cnt2, cnt2))
    return _arr


def symbol_fill(_arr):
    # todo handle remaining empty symbol rows
    print('symbol fill')
    arr = ad_hoc_fill(_arr)
    voodoo_fill(arr, 3)
    voodoo_fill(arr, 2)
    arr = ad_hoc_fill(arr)
    cnt2 = np.sum(arr['symbol'] == ' ')
    # print('{} symbols still empty'.format(cnt2))
    return arr

def upload_trans_list(_data):
    db_io.cur().executemany(db_io.upload_trans_stmt, _data)

def upload_trans_array(_data):
    upload_trans_list(_data.tolist())

def upload_trans_data(_data):
    '''
    INSERT OR IGNORE upload to database trans table
    :param _data: list of tuples which represent rows
    :return: None
    '''
    if isinstance(_data, list):
        upload_trans_list(_data)
    elif isinstance(_data, np.ndarray):
        upload_trans_array(_data)
    # print(type(_data))
    # print(_data)
    # print('\tuploading {} trans data'.format(len(_data)))
    # print(_data[0])
    # db_io.cur().executemany(db_io.upload_trans_stmt, _data.tolist()) 
    # print(db_io.cur())


# def load_trans_data(**darg):
#     key, val = darg.items()[0]
#     sql_stmt = db_io.select_trans_stmt(key), (val,)
#     print(sql_stmt)
#     return db_io.cur().execute(unicode(sql_stmt)).fetchall()

def load_trans_data():
    print('loading all trans data')
    tmp = db_io.con().cursor()
    tmp.execute(db_io.select_alltrans_stmt)
    if tmp is not None:
        return tmp.fetchall()
    else:
        raise Exception('no trans data recieved from db')

def np_load_trans_data(_symbol=None):
    if _symbol is None:
        tmp = db_io.cur().execute(db_io.select_alltrans_stmt)
        if tmp is not None:
            return np.array(tmp.fetchall(), dtype=db_tran_dtype)
    else:
        tmp = db_io.cur().execute(db_io.select_trans_stmt, (_symbol,))
        if tmp is not None:
            return np.array(tmp, dtype=db_tran_dtype)
        


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
    print('\tdata saved to CSV:\n\t\t{}'.format(out_rrsp_csv_file))

def dtype2cleancsv(row):
    # ISHARES 1 TO 3 YEAR TREASURY BOND ETF DIST      ON       4 SHS REC 12/29/14 PAY 12/31/14      , ,31-Dec-2014,31-Dec-2014,CAD,CASH DIV,0.00,CAD,0.000,0.15,
    tkns = row.split(',')
    # print(tkns)
    tran_date_str = tkns[2]
    tran_date_str = datetime.datetime.strptime(tkns[2], iso_datetime_format).date().strftime(rrsp_date_format)
    sett_date_str = tkns[3]
    sett_date_str = datetime.datetime.strptime(tkns[3], iso_datetime_format).date().strftime(rrsp_date_format)
    line = '{},{},{},{},{},{:.2f},{},{:.3f},{:.2f}'.format(tkns[1], tran_date_str, sett_date_str, tkns[4], tkns[5], float(tkns[6]), tkns[7], float(tkns[8]), float(tkns[9]))
    return line

def save_clean_csv(_arr):
    out_rrsp_csv_file = os.path.join(db_io.data_dir_path, 'CSV_DATA-{}.csv'.format(datetime.date.today().strftime(iso_date_format)))
    with open(out_rrsp_csv_file, 'w') as csv_file:
        csv_file.write(transaction_header + '\n')
        for tran in _arr:
            inline = '{},{},{},{},{},{},{},{},{},{}'.format(*tran.tolist())
            outline = dtype2cleancsv(inline) + '\n'
            csv_file.write(outline)
    print('\tdata saved to CSV:\n\t\t{}'.format(out_rrsp_csv_file))

def rebuild_trans_data():
    print('* * * remaking trans table')
    print('\tall trans data will be lost.\n\t\tENTER T to confirm')
    if user.upper() == 'T':
        # db_io.cur().execute(db_io.delete_trans_stmt)
        # db_io.con().commit()
        print('\ttable deleted')
        db_io.init_db()
        ts = load_rrsp_csv_trans()
        ts = symbol_fill(ts)
        data = ts.tolist()
        upload_trans_data(data)
        print('\ttable rebuilt and re-uploaded')
        print('\tsave data to csv?.\n\t\tENTER T to confirm')
        if user.upper() == 'T':
            # save_rrsp_csv_trans(ts)
            # np_ts = np_load_trans_data()
            # save_clean_csv(np_ts)
            pass
        else:
            print('\tCSV not saved')
    else:
        print('\ttable not deleted, left as is')


def test():
    pass
    arr = load_rrsp_csv_trans()
    if arr is not None:
        print(len(arr), type(arr))
    arr = symbol_fill(arr)
    # TEST DATA LOADERS
    upload_trans_data(arr)
    arr = np_load_trans_data(_symbol='RIM')
    if arr is not None:
        print(len(arr), type(arr))
    else:
        raise Exception()
    arr = np_load_trans_data()
    if arr is not None:
        print(len(arr), type(arr))
    else:
        raise Exception()
    arr = np_load_trans_data()
    if arr is not None:
        types = np.unique(arr['type'])
        print(types)
    else:
        raise Exception()
    data = load_trans_data()
    if data is not None:
        print('trans table loaded to memory ({} rows)'.format(len(data)))
    else:
        raise Exception()

if __name__ == '__main__':
    # rebuild_trans_data()

    data = load_trans_data()
    print(data)

    # test()
    # print(np.version.version)
    # print(dir(np))
    # print(np.__name__)
    # print(np.__path__)

    # REBUILD DATABASE AND CSV BACKUP
    # rebuild_trans_data()

    # PRINT ALL Transaction type's
    

    

    db_io.clean_up()
    pass