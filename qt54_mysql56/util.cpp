#include "util.h"

using namespace std;

void print(QStringList x)
{
  QString list = x.join("\n");
  qDebug() << list;
}
void print(QString x)
{
  qDebug() << x;
}
void print(QStringRef x)
{
  qDebug() << x.toString();
}
void print(string x)
{
  QString s(x.c_str());
  qDebug() << s;
}
void print(char x)
{
  QString s(x);
  qDebug() << s;
}
void print(double x)
{
  qDebug() << x;
}
void print(float x)
{
  qDebug() << x;
}
void print(int x)
{
  qDebug() << x;
}
void print(bool x)
{
  if (x) {
      qDebug() << "TRUE";
    } else {
      qDebug() << "FALSE";
    }
//  qDebug() << x;
}
