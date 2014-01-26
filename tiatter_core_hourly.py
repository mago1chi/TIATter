#coding:utf-8

import operator
import time
import fetchStream
import fileExtractor
import threading
import psycopg2

#Streaming APIを利用するために
#TwitterのOAuthトークンとOAuthシークレット，
#consumerキーとconsumerシークレットを設定
OAUTH_TOKEN = ""
OAUTH_SECRET = ""
CONSUMER_KEY = ""
CONSUMER_SECRET = ""

#ツイート文字列の一時保存先（外部プロセスによる形態素解析のため）
FILE_NAME = "eachTweet.txt"

DBPATH = "dbname=tiatterDB host=localhost user=postgres"

stream = fetchStream.FetchStream(OAUTH_TOKEN, OAUTH_SECRET, CONSUMER_KEY, CONSUMER_SECRET, DBPATH)
ext = fileExtractor.FileExtractor(DBPATH)

#スレッドの設定と開始
t1 = threading.Thread(target=stream.fetch)
t2 = threading.Thread(target=ext.extract_hourly, args=(FILE_NAME,))

t1.setDaemon(True)
t2.setDaemon(True)

t1.start()
t2.start()

#Twitter Streaming APIが終わるまでプログラムの終了を待つ
t1.join()
