#include <QCoreApplication>
#include <QSqlDatabase>
#include <QSqlQuery>
#include <QDate>

#include "util.h"

int main(int argc, char *argv[])
{
  QCoreApplication a(argc, argv);

  print(QSqlDatabase::drivers());

  QSqlDatabase db = QSqlDatabase::addDatabase("QMYSQL");
  db.setHostName("localhost");
  db.setDatabaseName("stock_stat");
  db.setUserName("quant");
  db.setPassword("2and2is5");
  bool ok = db.open();
  print(ok);

  QSqlQuery query;
  query.exec("SELECT * FROM stock_stat.prices");
  while (query.next()) {
      QString name = query.value(0).toString();
      QString date = query.value(1).toDate().toString();
      qDebug() << name << date;
    }

  return a.exec();
}
