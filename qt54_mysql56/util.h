#ifndef UTIL_H
#define UTIL_H

#include <QDebug>

using namespace std;


typedef QString str;

void print(QStringList str);
void print(QString x);
void print(QStringRef str);
void print(string str);
void print(char x);
void print(double x);
void print(float x);
void print(int x);
void print(bool x);

#endif // UTIL_H
