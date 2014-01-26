#coding:utf-8

from twitter import *
import operator
import re
import threading
import time
import datetime
import psycopg2
import shutil

#TwitterのStreaming APIを用いてツイートを取得し続ける。
#取得したツイートは、一度指定されたファイルへ書き込む。
class FetchStream():
	def __init__(self, oauth_token, oauth_secret, consumer_key, consumer_secret, dbPath):
		self.oauth_token = oauth_token
		self.oauth_secret = oauth_secret
		self.consumer_key = consumer_key
		self.consumer_secret = consumer_secret
		self.dbConnect = dbPath
		
	def fetch(self):
		twitter_stream = TwitterStream(auth=OAuth(self.oauth_token, self.oauth_secret, self.consumer_key, self.consumer_secret))
		
		while 1:
			iterator = twitter_stream.statuses.sample()

			for tweet in iterator:
				time.sleep(0.01)
				
				lang = tweet.get('lang', '')
				text = tweet.get('text', '')
			
				if text != '' and lang == 'ja' and not re.search(r'@[a-zA-Z0-9_]+', text):
					#text = re.sub(r'@.+\s', '', text)

					#RT、ハッシュタグ記号の除去
					text = re.sub(r'RT|rt|#', '', text)
					
					#URL除去
					text = re.sub(r"http[s]*://[a-zA-Z0-9./-_!*'();%s:@&=+$,%]+", '', text)

					#顔文字除去
					match_text    = '[0-9A-Za-zぁ-ヶ一-龠]'
					non_text      = '[^0-9A-Za-zぁ-ヶ一-龠]'
					allow_text    = '[ovっつ゜ニノ三二]'
					hw_kana       = '[ｦ-ﾟ]'
					open_branket  = '[\(∩（]'
					close_branket = '[\)∩）]'
					arround_face  = '(%s:' + non_text + '|' + allow_text + ')*'
					face          = '(%s!(%s:' + match_text + '|' + hw_kana + '){3,}).{3,}'
					face_char     = arround_face + open_branket + face + close_branket + arround_face

					text = re.sub(r"%s" % face_char, '', text)

					#カッコ記号を除去
					text = re.sub(r"[()\[\]]", '', text)

					#笑い記号"w"の除去
					text = re.sub(r"[wWｗW]{2,}", '', text)

					#半角カナの除去
					#text = re.sub(r"[ｱ-ﾝﾞﾟ]", '', text)
					
					#意味のわからない数字の羅列を除去(6桁-8桁のもの)
					text = re.sub(r"[0-9]{6,7}", '', text)
					
					#運勢占いのツイートを除去
					text = re.sub("運勢", '', text)
					text = re.sub("ﾗｯｷｰｱｲﾃﾑ", '', text)
					text = re.sub(r"[☆★]+", '', text)
			
					print(text)
					
					text = re.sub(r'\n', '', text)
					
					#単語カウント用DBへ取得したツイートを一時的に登録
					conn = psycopg2.connect(self.dbConnect)
					connCur = conn.cursor()
					
					connCur.execute("select relname from pg_class where relkind='r' and relname='each_tmp_tweet'")

					if not connCur.fetchone():
						connCur.execute('''create table each_tmp_tweet(
							tweet text,
							time timestamp)''')
						
					tweetTime = datetime.datetime.now()
					
					connCur.execute("insert into each_tmp_tweet values (%s, %s)", (text, tweetTime))
					
					conn.commit()
					connCur.close()
					conn.close()
			
					#各ツイートをDBへ登録
					conn = psycopg2.connect(self.dbConnect)
					connCur = conn.cursor()

					connCur.execute("select relname from pg_class where relkind='r' and relname='each_tweet'")

					if not connCur.fetchone():
						connCur.execute('''create table each_tweet(
							tweet text,
							time timestamp)''')
			
					connCur.execute("insert into each_tweet values (%s, %s)", (text, tweetTime))

					conn.commit()
					connCur.close()
					conn.close()
		
