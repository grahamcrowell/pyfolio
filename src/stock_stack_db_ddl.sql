-- cd /usr/local/mysql-5.6.23-osx10.8-x86_64/bin 
-- ./mysql -h "localhost" -u "quant" -p "stockstat"

SELECT CURRENT_USER();
SELECT DATABASE();

CREATE DATABASE IF NOT EXISTS stock_stat;

CONNECT stock_stat;

-- CREATE TABLE IF NOT EXISTS trans (desc TEXT, symbol TEXT, tran_date DATE, settle_date DATE, acct_currency TEXT, type TEXT, qty REAL, price_currency TEXT, price REAL, amount REAL);
CREATE TABLE IF NOT EXISTS stock_stat.trans (note CHAR(100), symbol CHAR(10), tran_date DATE, settle_date DATE, acct_currency CHAR(10), type CHAR(15), qty INTEGER, price_currency CHAR(10), price FLOAT, amount FLOAT);
-- CREATE TABLE IF NOT EXISTS prices (symbol TEXT, date DATE, open REAL, high REAL, low REAL, close REAL, volume INTEGER, adj_close REAL, PRIMARY KEY (symbol, date));
CREATE TABLE IF NOT EXISTS stock_stat.prices (symbol CHAR(10), date DATE, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume INTEGER, adj_close FLOAT, PRIMARY KEY (symbol, date));
