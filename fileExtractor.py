#coding:utf-8

import operator
import datetime
import re
import time
import parse
import dbUpdater
import threading
import tsTfIdf
import jsonCreator
import psycopg2
import threading

class FileExtractor():
	def __init__(self, dbPath):
		self.dbConnect = dbPath

	#トレンド語を検出し、結果を共起語と共にファイルへ書き込む
	#tfidfを使用
	def extTfIdf(self, bottomTime, upperTime):
		trendExtractor = tsTfIdf.TsTfIdf(self.dbConnect)
		
		trendDic = trendExtractor.calcTfIdf(bottomTime, 6, False)
		
		#tfidfについての処理
		#検出した特徴語と共起頻度の高い単語を検出
		cooccurTerms = {}
		objAry = ['名詞']
	
		mecab = parse.Parser("tweet.txt")
	
		conn = psycopg2.connect(self.dbConnect)
		connCur = conn.cursor()
	
		for term in trendDic.keys():
			time.sleep(0.001)
			
			eachCooccur = {}
	
			connCur.execute("select tweet from each_tweet where time>=%s and time<%s", (bottomTime, upperTime))
	
			for eachText in connCur:
				if re.search(term, eachText[0]):
					f = open('tweet.txt', 'w')
					f.write(eachText[0])
					f.close()
	
					termCount = mecab.getTermCount(objAry)
	
					for word, count in termCount.items():
						if word != term:
							if not word in eachCooccur:
								eachCooccur[word] = count
							else:
								eachCooccur[word] += count
	
			cooccurTerms[term] = eachCooccur
		
		#結果をファイルへ書き込み
		jsonCreator.createJson('tfidf.json', trendDic, cooccurTerms, bottomTime)
	
		connCur.close()
		conn.close()
	
	#time-sensitive tfidfを使用
	def extTSTfIdf(self, bottomTime, upperTime):
		trendExtractor = tsTfIdf.TsTfIdf(self.dbConnect)
		
		trendDic = trendExtractor.calcTSTfIdf(bottomTime, 6, False)
		
		#ts-tfidfについての処理
		#検出した特徴語と共起頻度の高い単語を検出
		cooccurTerms = {}
		objAry = ['名詞']
	
		mecab = parse.Parser("tweet_ts.txt")
	
		conn = psycopg2.connect(self.dbConnect)
		connCur = conn.cursor()
	
		for term in trendDic.keys():
			time.sleep(0.001)
			eachCooccur = {}
	
			connCur.execute("select tweet from each_tweet where time>=%s and time<%s", (bottomTime, upperTime))
	
			for eachText in connCur:
				if re.search(term, eachText[0]):
					f = open('tweet_ts.txt', 'w')
					f.write(eachText[0])
					f.close()
	
					termCount = mecab.getTermCount(objAry)
	
					for word, count in termCount.items():
						if word != term:
							if not word in eachCooccur:
								eachCooccur[word] = count
							else:
								eachCooccur[word] += count
	
			cooccurTerms[term] = eachCooccur
		
		#結果をファイルへ書き込み
		jsonCreator.createJson('tsTfIdf.json', trendDic, cooccurTerms, bottomTime)
	
		connCur.close()
		conn.close()
	
	
	#指定したファイル名中に記載されている文章を用いて、
	#MeCabから形態素解析をおこない、文字カウントを
	#データベースへ保存。
	def extract(self, fileName, timeInterval):
		sepTime = datetime.datetime.now()
	
		i = 0
		while 1:
			currentTime = datetime.datetime.now()
	
			#区切り時間からtimeInterval秒経過していれば、新たに時間を区切る
			if datetime.timedelta.total_seconds(currentTime - sepTime) >= timeInterval:
				sepTime = sepTime + datetime.timedelta(seconds=timeInterval)
	
			conn = psycopg2.connect(self.dbConnect)
			connCur = conn.cursor()
			
			connCur.execute('select tweet from each_tmp_tweet')
			
			mecab = parse.Parser(fileName)
			objAry = ["名詞"]
			
			termCount = {}
			for eachTweet in connCur:
				f = open(fileName, 'w')
				f.write(eachTweet[0])
				f.close()
				
				termList = mecab.getTerm(objAry)

				for eachTerm in termList:
					if not eachTerm in termCount:
						termCount[eachTerm] = 0
					
					termCount[eachTerm] += 1
				
				time.sleep(0.0001)
			
			db = dbUpdater.DBUpdater(self.dbConnect)
			db.regist(termCount, sepTime)
			
			connCur.execute('delete from each_tmp_tweet')
			conn.commit()
			
			connCur.close()
			conn.close()
	
	
	#指定したファイル名中に記載されている文章を用いて、
	#MeCabから形態素解析をおこない、文字カウントを
	#データベースへ保存。
	def extract_daily(self, fileName):
		sepTime = datetime.datetime.now()
		sepTime = sepTime - datetime.timedelta(hours=sepTime.hour, minutes=sepTime.minute, seconds=sepTime.second, microseconds=sepTime.microsecond)
	
		while 1:
			currentTime = datetime.datetime.now()
	
			#sepTime計測から日を跨いでいれば、sepTimeを更新する
			if currentTime.day != sepTime.day:
				conn = psycopg2.connect(self.dbConnect)
				connCur = conn.cursor()
				
				connCur.execute("select distinct time from term_counts")
				
				rows = connCur.fetchall()
				
				#30日分より多いデータを削除
				if len(rows) > 30:
					connCur.execute("select distinct time from term_counts order by time desc limit 1 offset 30")
					oldestTime = connCur.fetchone()
					
					connCur.execute("delete from term_counts where time<=%s", (oldestTime[0],))
					conn.commit()
					
					oldestTweetTime = oldestTime[0] + datetime.timedelta(days=1)
					
					connCur.execute("delete from each_tweet where time<%s", (oldestTweetTime,))
					conn.commit()
		
				connCur.close()
				conn.close()
	
				sepTime = datetime.datetime.now()
				sepTime = sepTime - datetime.timedelta(hours=sepTime.hour, minutes=sepTime.minute, seconds=sepTime.second, microseconds=sepTime.microsecond)
	
			conn = psycopg2.connect(self.dbConnect)
			connCur = conn.cursor()
			
			connCur.execute('select tweet from each_tmp_tweet')
			
			mecab = parse.Parser(fileName)
			objAry = ["名詞"]
			
			termCount = {}
			for eachTweet in connCur:
				f = open(fileName, 'w')
				f.write(eachTweet[0])
				f.close()
				
				termList = mecab.getTerm(objAry)

				for eachTerm in termList:
					if not eachTerm in termCount:
						termCount[eachTerm] = 0
					
					termCount[eachTerm] += 1
					
				time.sleep(0.0001)
			
			db = dbUpdater.DBUpdater(self.dbConnect)
			db.regist(termCount, sepTime)
			
			connCur.execute('delete from each_tmp_tweet')
			conn.commit()
			
			connCur.close()
			conn.close()
	

	#指定したファイル名中に記載されている文章を用いて、
	#MeCabから形態素解析をおこない、文字カウントを
	#データベースへ保存。
	def extract_hourly(self, fileName):
		sepTime = datetime.datetime.now()
		sepTime = sepTime - datetime.timedelta(minutes=sepTime.minute, seconds=sepTime.second, microseconds=sepTime.microsecond)
	
		while 1:
			currentTime = datetime.datetime.now()
	
			#sepTime計測から時間を跨いでいれば、sepTimeを更新する
			if currentTime.hour != sepTime.hour:
				conn = psycopg2.connect(self.dbConnect)
				connCur = conn.cursor()
				
				connCur.execute("select distinct time from term_counts")
				
				rows = connCur.fetchall()
				
				#72時間より多い分のデータを削除
				if len(rows) > 72:
					connCur.execute("select distinct time from term_counts order by time desc limit 1 offset 72")
					oldestTime = connCur.fetchone()
					
					connCur.execute("delete from term_counts where time<=%s", (oldestTime[0],))
					conn.commit()
		
					oldestTweetTime = oldestTime[0] + datetime.timedelta(hours=1)
					
					connCur.execute("delete from each_tweet where time<%s", (oldestTweetTime,))
					conn.commit()
		
				connCur.close()
				conn.close()
				
				sepTime = datetime.datetime.now()
				sepTime = sepTime - datetime.timedelta(minutes=sepTime.minute, seconds=sepTime.second, microseconds=sepTime.microsecond)
	
			conn = psycopg2.connect(self.dbConnect)
			connCur = conn.cursor()
			
			connCur.execute('select tweet from each_tmp_tweet')
			
			mecab = parse.Parser(fileName)
			objAry = ["名詞"]
			
			termCount = {}
			for eachTweet in connCur:
				f = open(fileName, 'w')
				f.write(eachTweet[0])
				f.close()
				
				termList = mecab.getTerm(objAry)

				for eachTerm in termList:
					if not eachTerm in termCount:
						termCount[eachTerm] = 0
					
					termCount[eachTerm] += 1
				
				time.sleep(0.0001)
			
			db = dbUpdater.DBUpdater(self.dbConnect)
			db.regist(termCount, sepTime)
			
			connCur.execute('delete from each_tmp_tweet')
			conn.commit()
			
			connCur.close()
			conn.close()
