#coding:utf-8

import os
import re

#外部プロセスとしてmecabを呼び出し，任意の文字列を形態素解析する
class Parser():
	def __init__(self, fileName):
		self.fileName = fileName

	#形態素解析した結果を返すだけのメソッド
	def parse(self, objAry):
		cmd = 'mecab ' + self.fileName
		proc = os.popen(cmd)
		result = proc.read()
		proc.close()

		subResult = re.sub(r'\s*\t|\n', ',', result)

		resAry = subResult.split(',')

		parsedDic = {}
		for obj in objAry:
			for i in range(len(resAry)):
				if resAry[i] == obj:
					word = resAry[i-1]

				if not word in parsedDic:
					parsedDic[word] = obj

		return parsedDic

	#形態素解析した各単語と，単語それぞれの出現頻度を返すメソッド
	def getTermCount(self, objAry):
		cmd = 'mecab ' + self.fileName
		proc = os.popen(cmd)
		result = proc.read()
		proc.close()

		subResult = re.sub(r'\s*\t|\n', ',', result)

		resAry = subResult.split(',')

		parsedDic = {}
		for obj in objAry:
			for i in range(len(resAry)):
				if resAry[i] == obj:
					word = resAry[i-1]

					if not word in parsedDic:
						parsedDic[word] = 0

					parsedDic[word] += 1

		return parsedDic

	#形態素解析した単語をリスト形式で返すメソッド
	def getTerm(self, objAry):
		cmd = 'mecab ' + self.fileName
		proc = os.popen(cmd)
		result = proc.read()
		proc.close()

		subResult = re.sub(r'\s*\t|\n', ',', result)

		resAry = subResult.split(',')

		parsedList = []
		for obj in objAry:
			for i in range(len(resAry)):
				if resAry[i] == obj:
					word = resAry[i-1]

					if not word in parsedList:
						parsedList.append(word)


		return parsedList
