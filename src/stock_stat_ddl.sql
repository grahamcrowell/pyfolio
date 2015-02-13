-- cd /usr/local/mysql-5.6.23-osx10.8-x86_64/bin 
-- ./mysql -h "localhost" -u "quant" -p "stockstat"
-- ./mysqldump -u quant -p2and2is5  --tab=/tmp stock_stat  --fields-terminated-by=','

-- /usr/local/mysql-5.6.23-osx10.8-x86_64/bin/mysql -h "localhost" -u "quant" -p2and2is5 "stock_stat" < /Users/grahamcrowell/Dropbox/github_local/pyfolio/src/stock_stat_ddl.sql


-- CREATE USER 'quant'@'localhost' IDENTIFIED BY '2and2is5';
-- GRANT ALL ON *.* TO 'quant'@'localhost';
-- GRANT FILE ON *.* TO 'quant'@'localhost';

SELECT CURRENT_USER();
SELECT DATABASE();

CREATE DATABASE IF NOT EXISTS stock_stat;

CONNECT stock_stat;

CREATE TABLE IF NOT EXISTS stock_stat.trans (note CHAR(100), symbol CHAR(10), tran_date DATE, settle_date DATE, acct_currency CHAR(10), type CHAR(15), qty INTEGER, price_currency CHAR(10), price FLOAT, amount FLOAT);
CREATE TABLE IF NOT EXISTS stock_stat.prices (symbol CHAR(10), date DATE, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume INTEGER, adj_close FLOAT, PRIMARY KEY (symbol, date));

COMMIT;