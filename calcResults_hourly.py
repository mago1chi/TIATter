#coding:utf-8

import operator
import time
import psycopg2
import threading
import tsTfIdf
import fileExtractor
import datetime

#トレンド解析に用いるデータベースを指定
DBPATH = "dbname=tiatterDB host=localhost user=postgres"

#最も新しい時間についてトレンド解析をするために，直近の時間を取得
conn = psycopg2.connect(DBPATH)
cur = conn.cursor()

cur.execute("select time from term_counts group by time order by time desc limit 1")

result = cur.fetchone()

bottomTime = result[0]

upperTime = bottomTime + datetime.timedelta(hours=1)

ext = fileExtractor.FileExtractor(DBPATH)

#ここ１時間のトレンドを計算
ext.extTSTfIdf(bottomTime, upperTime)

