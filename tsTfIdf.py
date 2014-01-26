#coding:utf-8

import psycopg2
import datetime
import math

#指定した時間におけるトレンドを計算
class TsTfIdf():
	def __init__(self, dbPath):
		self.dbPath = dbPath

	#出現時間の分散を考慮しないtime-sensitive TFxIDFを計算
	def calcTfIdf(self, targetTime, rankLimit=10, norm=False):
		conn = psycopg2.connect(self.dbPath)
		connCur = conn.cursor()

		#インターバルtargetTime中における、各単語の出現数を取得
		connCur.execute("select term, count from term_counts where time=%s", (targetTime,))

		eachTermCounts = {}
		eachTICounts = {}
		termSum = 0

		for row in connCur:
			eachTermCounts[row[0]] = row[1]
			termSum += row[1]

		#インターバルtargetTime中に出現する各単語が、いくつのインターバルに
		#出現するかをカウント
		for key in eachTermCounts.keys():
			connCur.execute("select * from  term_counts where term=%s and time<=%s", (key, targetTime))
			eachTICounts[key] = len(connCur.fetchall())

		#targetTime以下にあるインターバルの数を取得
		connCur.execute("select count(*) from term_counts where time<=%s group by time", (targetTime,))
		intervalNum = len(connCur.fetchall())

		#各単語について、取得したカウントを用いてtime-sensitive TFxIDFを計算
		tsTfIdf = {}

		for key in eachTermCounts.keys():
			tsTfIdf[key] = (eachTermCounts[key] / termSum) * math.log(intervalNum / (1+eachTICounts[key]))

		#正規化用の係数
		maxValue = max(tsTfIdf.values())

		#上位rankLimit番目までの単語を抽出
		calcResults = {}

		i = 0
		for key, value in sorted(tsTfIdf.items(), key=lambda x:x[1], reverse=True):
			calcResults[key] = value

			i += 1
			if i is rankLimit-1:
				break

		#norm=Trueのとき、正規化した結果を算出
		if norm:
			for key in calcResults.keys():
				calcResults[key] = calcResults[key] / maxValue

		return calcResults


	#発生時間の分散を考慮したtime-sensitive TFxIDFを計算
	def calcTSTfIdf(self, targetTime, rankLimit=10, norm=False):
		conn = psycopg2.connect(self.dbPath)
		connCur = conn.cursor()

		#インターバルtargetTime中における、各単語の出現数を取得
		connCur.execute("select term, count from term_counts where time=%s", (targetTime,))

		eachTermCounts = {}
		eachTICounts = {}
		eachSigma = {}
		termSum = 0

		for row in connCur:
			eachTermCounts[row[0]] = row[1]
			termSum += row[1]

		#インターバルtargetTime中に出現する各単語が、いくつのインターバルに
		#出現するかをカウント
		for key in eachTermCounts.keys():
			connCur.execute("select time from term_counts where term=%s and time<=%s", (key, targetTime))
			eachTI = connCur.fetchall()
			eachTICounts[key] = len(eachTI)

			#各単語が出現した時間の分散を算出
			if len(eachTI) is 1:
				eachSigma[key] = 0
			else:
				timeSum = 0
				timeList = []
				for t in eachTI:
					eachTime = int(t[0].strftime("%Y%m%d%H%M%S"))
					timeSum += eachTime
					timeList.append(eachTime)

				timeAve = timeSum / len(timeList)

				powSum = 0
				for t in timeList:
					powSum += math.pow(t - timeAve, 2)

				eachSigma[key] = math.sqrt(powSum / (len(eachTI) - 1))

		#各単語の分散を正規化
		varMax = max(eachSigma.values())

		for key, value in eachSigma.items():
			eachSigma[key] = value / varMax

		#targetTime以下にあるインターバルの数を取得
		connCur.execute("select count(*) from term_counts where time<=%s group by time", (targetTime,))
		intervalNum = len(connCur.fetchall())

		#各単語について、取得したカウントを用いてtime-sensitive TFxIDFを計算
		tsTfIdf = {}

		for key in eachTermCounts.keys():
			tsTfIdf[key] = (eachTermCounts[key] / termSum) * math.log(intervalNum / (1+eachTICounts[key])) * (1 - eachSigma[key])

		#正規化用の係数
		maxValue = max(tsTfIdf.values())

		#上位rankLimit番目までの単語を抽出
		calcResults = {}

		i = 0
		for key, value in sorted(tsTfIdf.items(), key=lambda x:x[1], reverse=True):
			calcResults[key] = value

			i += 1
			if i is rankLimit-1:
				break

		#norm=Trueのとき、正規化した結果を算出
		if norm:
			for key in calcResults.keys():
				calcResults[key] = calcResults[key] / maxValue

		return calcResults

