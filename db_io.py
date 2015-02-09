__author__ = 'grahamcrowell'

import datetime
import sqlite3
import os
import zipfile

print('\ncurrent directory:\n\t{}'.format(os.getcwd()))
data_dir_name = 'data'
data_dir_path = os.path.join(os.getcwd(), data_dir_name)
print('data directory:\n\t{}'.format(data_dir_path))
bkup_dir_name = 'bkup'
bkup_dir_path = os.path.join(data_dir_path, bkup_dir_name)
print('bkup directory:\n\t{}'.format(bkup_dir_path))

db_name = 'quant'
print('database name:\n\t{}\n'.format(db_name))
db_filename = db_name + '.db'
db_rel_path = os.path.join(data_dir_name,db_filename)
db_fullpath = os.path.join(data_dir_path, db_filename)
db_file = db_fullpath

sql_name = db_name + '.{}.sql'.format(datetime.date.today().isoformat())
sql_rel_path = os.path.join(data_dir_name,sql_name)
sql_fullpath = os.path.join(bkup_dir_path, sql_name)

zip_name = os.path.join(bkup_dir_path, db_name + '.{}.zip'.format(datetime.date.today().isoformat()))
zip_rel_path = os.path.join(data_dir_name,zip_name)
db_zip = os.path.join(bkup_dir_path, zip_name)



"""
price table
"""
create_price_stmt = '''CREATE TABLE IF NOT EXISTS prices (symbol text, date date, open real, high real, low real, close real, volume integer, adj_close real, PRIMARY KEY (symbol, date)) '''
# create_price_stmt = '''CREATE TABLE IF NOT EXISTS prices (symbol text, date date, open real, high real, low real, close real, volume integer, adj_close real)'''
delete_price_stmt = '''DROP TABLE prices'''
select_price_stmt = '''SELECT * FROM prices WHERE symbol=?'''
lastup_price_stmt = '''SELECT MAX(date) FROM prices WHERE symbol=?'''
upload_price_stmt = '''INSERT OR IGNORE INTO prices VALUES (?,?,?,?,?,?,?,?)'''

"""
transaction table
"""
create_trans_stmt = '''CREATE TABLE IF NOT EXISTS trans (desc text, symbol text, tran_date date, settle_date date, acct_currency text, type text, qty real, price_currency text, price real, amount real)'''
upload_trans_stmt = '''INSERT OR IGNORE INTO trans VALUES (?,?,?,?,?,?,?,?,?,?)'''
select_trans_stmt = '''SELECT * FROM trans WHERE {}=?'''.format
select_trans_stmt = '''SELECT * FROM trans WHERE symbol=?'''
select_alltrans_stmt = '''SELECT * FROM trans'''
delete_trans_stmt = '''DROP TABLE trans'''

_con = None
_cur = None


def init_db():
    global _con
    global _cur
    if _con is None:
        print('connecting to {}\n'.format(db_name))
        _con = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES)
    if _cur is None:
        _cur = _con.cursor()
    _con.execute(create_price_stmt)
    _con.execute(create_trans_stmt)


def remake_db():
    print('* * * remaking db'.format(db_name))
    user = raw_input('\tall database data will be lost.\n\t\tENTER T to confirm')
    if user.upper() == 'T':
        cur().execute(delete_price_stmt)
        cur().execute(delete_trans_stmt)
        print('\tdatabase deleted')
        init_db()
        print('\tdatabase rebuilt')
    else:
        print('\tdatabase NOT deleted')


def clean_up():
    global _con
    global _cur
    if _cur is not None:
        _cur.close()
    if _con is not None:
        _con.commit()
        _con.close()


def cur():
    global _cur
    if _cur is not None:
        return _cur
    else:
        init_db()
        return _cur


def con():
    global _con
    if _con is not None:
        return _con
    else:
        init_db()
        return _con

def bkup_db_sql():
    print('\tsaving {} to {}'.format(db_rel_path,sql_rel_path))
    init_db()
    con().commit()
    with open(sql_rel_path, 'w') as f:
        for line in con().iterdump():
            f.write('%s\n' % line)

def bkup_db_zip():
    print('\tsaving {} to {}'.format(db_rel_path,zip_name))
    # os.chdir(data_dir_path)
    with zipfile.ZipFile(zip_rel_path, 'w') as myzip:
        myzip.write(db_rel_path)
        if not os.path.isfile(sql_rel_path):
            bkup_db_sql()
        myzip.write(sql_rel_path)
    os.remove(sql_rel_path)



def dump_db():
    print('dumping DB file: {} . . .'.format(db_rel_path))
    
    bkup_db_sql()
    bkup_db_zip()



if __name__ == '__main__':
    dump_db()

    # mtime = os.path.getmtime(path)


