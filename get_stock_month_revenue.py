#!/usr/bin/python
# -*- coding: utf-8 -*-

#
#        GOAL: fetch month revenue for taiwan stock month 
#       USAGE: ./get_stock_month_revenue.py -i <id> -s <startyearmon> -e <endyearmon>
# OBSERVE URL: http://mops.twse.com.tw/mops/web/t05st10 
#   DB FORMAT:
#
#      id   年月  本月  去年同期 年增率 本年累計 去年累計 年增率
#     ---------------------------------------------------------------
#       1  10102  xxxx    yyyy    aaaa     bbbb   cccc     dddd
#
#     LICENSE: MIT, Copyright (c) 2014 Enzo Wang
#    
#    Permission is hereby granted, free of charge, to any person
#    obtaining a copy of this software and associated documentation
#    files (the "Software"), to deal in the Software without
#    restriction, including without limitation the rights to use,
#    copy, modify, merge, publish, distribute, sublicense, and/or sell
#    copies of the Software, and to permit persons to whom the
#    Software is furnished to do so, subject to the following
#    conditions:
#    
#    The above copyright notice and this permission notice shall be
#    included in all copies or substantial portions of the Software.
#    
#    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
#    OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
#    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
#    HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
#    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
#    OTHER DEALINGS IN THE SOFTWARE.
#

import pycurl
import os
import sys, getopt
import sqlite3
import time
from bs4 import BeautifulSoup

stockNum = 0
start_year = 0
start_month = 0
end_year = 0
end_month = 0
fn='fetch_data.html'
tbln="mr" # "mr" stands for "month revenue"

def arg_check(argv):
    stockid = 0
    start_yearmon = 0
    end_yearmon = 0
    try:
        opts, args = getopt.getopt(argv,"hi:s:e:",["id=","startyearmon=","endyearmon="])
    except getopt.GetoptError:
        print 'get_stock_month_revenue.py -i <id> -s <startyearmon> -e <endyearmon>'
        print 'ex: get_stock_month_revenue.py -i 3380 -s 9901 -e 10212'
        sys.exit(2)
    if not argv:
        print 'get_stock_month_revenue.py -i <id> -s <startyearmon> -e <endyearmon>'
        print 'ex: get_stock_month_revenue.py -i 3380 -s 9901 -e 10212'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'get_stock_month_revenue.py -i <id> -s <startyearmon> -e <endyearmon>'
            print 'ex: get_stock_month_revenue.py -i 3380 -s 9901 -e 10212'
            sys.exit()
        elif opt in ("-i", "--id"):
            stockid = arg
        elif opt in ("-s", "--startyearmon"):
            start_yearmon = arg
        elif opt in ("-e", "--endyearmon"):
            end_yearmon = arg

    global stockNum, start_year, start_month, end_year, end_month, dbn
    stockNum = int(stockid)
    start_year = int(start_yearmon[:-2])
    start_month = int(start_yearmon[-2:])
    end_year = int(end_yearmon[:-2])
    end_month = int(end_yearmon[-2:])
    dbn=str(stockNum)+'_month_revenue.db'
    print 'Get stock %d from %d-%d to %d-%d' % (stockNum,start_year,start_month,end_year,end_month)

if __name__ == "__main__":
    arg_check(sys.argv[1:])

#
# database functions
#
def db_connect(name):
    create = not os.path.exists(name)
    conn = sqlite3.connect(name)
    if create:
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE " + tbln + " ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, "
            "yearmonth INTEGER NOT NULL, "
            "month INTEGER NOT NULL, "
            "premonth INTEGER NOT NULL, "
            "monthgain REAL NOT NULL, "
            "accumonth INTEGER NOT NULL, "
            "preaccumonth INTEGER NOT NULL, "
            "accumonthgain REAL NOT NULL) ")
        conn.commit()
    return conn

def db_add(conn, yearmonth, month, premonth, accumonth, preaccumonth):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO " + tbln + " "
                    "(yearmonth, month, premonth, monthgain, accumonth, preaccumonth, accumonthgain) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (yearmonth, month, premonth, round(float(month)/premonth-1,2), accumonth, preaccumonth, round(float(accumonth)/preaccumonth-1,2)))
    conn.commit()

def db_is_existed(conn, yearmonth):
    cursor = conn.cursor()
    result = cursor.execute("SELECT * FROM " + tbln + " WHERE yearmonth=" + str(yearmonth))
    if not cursor.fetchone():
        #print "not found for %d" % yearmonth
        return 0
    else:
        #print "found for %d" % yearmonth
        return 1
#
# utility functions
#
def monthStrGet(_month):
    if (_month<10):
        return '0'+str(_month)
    else: 
        return str(_month)

def postStrGet(stocknum_, year_, month_):
    yearmonth_ = str(year_)+monthStrGet(month_) 
    return ('encodeURIComponent=1&run=Y&step=0&yearmonth='+yearmonth_+
            '&colorchg=&TYPEK=sii%20&co_id='+str(stocknum_)+
            '&off=1&year='+str(year_)+
            '&month='+monthStrGet(month_)+'&firstin=true')

def hasConsolidatedRevenue():
    soup = BeautifulSoup(open(fn))
    result = soup.find_all(text='合併營業收入淨額')
    return len(result)

#
# get desired data
#
def getData(items_):
    soup = BeautifulSoup(open(fn))
    index = 1 if hasConsolidatedRevenue() else 0
    val=[]
    for idx,item in enumerate(items_):
        title = soup.find_all("th", text=item)
        data = title[index].find_next_sibling("td")
        data_str = unicode(data.string)
        data_num = data_str.replace(",","")
        print("%10s %d" % (item,  int(data_num)))
        val.append(int(data_num))
    return val

def fetchAndSaveData(conn, stockNum, year, month):
    obvItems = ['本月','去年同期','本年累計','去年累計']
    poststr = postStrGet(stockNum, year, month)

    f = open(fn, 'w')
    c = pycurl.Curl()
    c.setopt(c.URL, 'http://mops.twse.com.tw/mops/web/t05st10'+ ('_ifrs' if (year>=102) else ''))
    c.setopt(c.POSTFIELDS, poststr)
    c.setopt(c.WRITEFUNCTION, f.write)
    c.perform()
    c.close()
    f.close()
    time.sleep(2.0)
     
    data = getData(obvItems)
    db_add(conn,year*100+month,data[0],data[1],data[2],data[3])

#
# main logic
#

# check page stored file and db file exist
if os.path.isfile(fn):
    os.remove(fn)
else: 
    print('Error: %s file not found' % fn)

#conn = sqlite3.connect(":memory:")
conn = db_connect(dbn)

for y in range (start_year,end_year+1):
    if (y == start_year):
        obv_start_month = start_month
    else:
        obv_start_month = 1;
    if (y == end_year):
        obv_end_month = end_month
    else:
        obv_end_month = 12

    for m in range(obv_start_month, obv_end_month+1):
        if db_is_existed(conn, y*100+m):
            print "skip for year %d month %d" % (y,m)
            continue
        print "fetch year %d month %d" % (y,m)
        fetchAndSaveData(conn, stockNum, y, m)

