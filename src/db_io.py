__author__ = 'grahamcrowell'

# todo: phase of buffered cursors

import os,datetime,subprocess
import mysql.connector


def set_cwd_pyfolio_root():
    print('\ncurrent directory:\n\t{}'.format(os.getcwd()))
    if os.path.split(os.getcwd())[1] != 'pyfolio':
        dir_names = os.getcwd().split('/')[1:]
        print('\t\tsearching for pyfolio in current working directory')
        if 'pyfolio' not in dir_names:
            raise Exception('\nERROR: root of source should be pyfolio')
        n = len(dir_names)-dir_names.index('pyfolio')-1
        for i in range(n):
            os.chdir('../')
        print('current directory:\n\t{}'.format(os.getcwd()))
    assert os.path.split(os.getcwd())[1] == 'pyfolio'
    
set_cwd_pyfolio_root()

data_dir_name = 'data'
data_dir_path = os.path.join(os.getcwd(), data_dir_name)
print('data directory:\n\t{}'.format(data_dir_path))
assert os.path.isdir(data_dir_path)
bkup_dir_name = 'bkup'
bkup_dir_path = os.path.join(data_dir_path, bkup_dir_name)
print('bkup directory:\n\t{}'.format(bkup_dir_path))
assert os.path.isdir(bkup_dir_path)
db_name = 'stock_stat'
print('database name:\n\t{}\n'.format(db_name))
# zip backup path for sql dump and new csv backup
zip_name = os.path.join(bkup_dir_path, db_name + '.{}.zip'.format(datetime.date.today().isoformat()))
zip_rel_path = os.path.join(data_dir_name,zip_name)
db_zip = os.path.join(bkup_dir_path, zip_name)
# sql dump
sql_name = db_name + '.{}.sql'.format(datetime.date.today().isoformat())
sql_rel_path = os.path.join(data_dir_name,sql_name)
sql_fullpath = os.path.join(bkup_dir_path, sql_name)








"""
price table
"""
select_price_stmt = '''SELECT * FROM prices WHERE symbol=%s'''
lastup_price_stmt = '''SELECT MAX(date) FROM prices WHERE symbol=%s'''
upload_price_stmt = '''INSERT IGNORE INTO prices VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''
"""
transaction table
"""
upload_trans_stmt = '''INSERT IGNORE INTO trans VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
select_trans_stmt = '''SELECT * FROM trans WHERE symbol=%s'''
select_alltrans_stmt = '''SELECT * FROM trans'''

_con = None

# TODO formatted server and table info
def display_info():
    info = ''
    info += 'MySQL server version\n\t{}'.format('.'.join(map(str,con().get_server_version())))
    print(info)
    print(con().cmd_statistics())
    print(con().server_host)
    print(con().user)
    print(con().server_port)
    print(con().database)

def init_db():
    global _con
    if _con is None:
        print('connecting to {}\n'.format(db_name))
        _con = mysql.connector.connect(user='quant', database=db_name, password='2and2is5',buffered=True)

def clean_up(): 
    global _con
    # global _cur
    # if _cur is not None:
        # _cur.close()
    if _con is not None:
        _con.commit()
        _con.close()

def cur():
    return con().cursor()

def con():
    global _con
    if _con is not None:
        return _con
    else:
        init_db()
        return _con

def run_ddl():
    cmd = ['/usr/local/mysql-5.6.23-osx10.8-x86_64/bin/mysql','-h "localhost"', '-u "quant"', '-p2and2is5', "stock_stat", "./src/stock_stat_ddl.sql"]
    p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
    p.communicate()

init_db()
# display_info()
if __name__ == '__main__':

    # print(con())
    # print(cur())
    # dump_db()
    # foo()
    pass


