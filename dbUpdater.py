#coding:utf-8

import psycopg2
import datetime
import threading

#取得した単語と出現数の組み合わせをDBへ登録
class DBUpdater():
	def __init__(self, dbPath):
		self.dbPath = dbPath
	
	#同じタイムスタンプ内に、同一の単語があれば出現数を加算
	#新しい単語であれば新規に登録する
	def regist(self, termDict, timeStamp):
		conn = psycopg2.connect(self.dbPath)
		connCur = conn.cursor()
		
		connCur.execute("select relname from pg_class where relkind='r' and relname='term_counts'")
		if not connCur.fetchone():
			connCur.execute('''create table term_counts(
				term text,
				count int,
				time timestamp)''')
			
		for word, count in termDict.items():
			connCur.execute("select count from term_counts where term=%s and time=%s", (word, timeStamp))
			oneCount = connCur.fetchone()
			
			if oneCount:
				newCount = oneCount[0] + count
				connCur.execute("update term_counts set count=%s where term=%s and time=%s", (newCount, word, timeStamp))

			else:
				connCur.execute("insert into term_counts values (%s, %s, %s)", (word, count, timeStamp))
				
		conn.commit()
		connCur.close()
		conn.close()

