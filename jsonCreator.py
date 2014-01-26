#coding:utf-8

import json
import datetime
import copy

def createJson(filePath, trendDic, cooccurTerms, timestamp):
	f = open(filePath, 'r')

	strTime = timestamp.strftime("%Y %m %d %H:%M:%S")
	#intTime = int(timestamp.strftime("%Y%m%d%H%M%S"))

	newJsonData = {}

	#既にfilePathのjsonにデータがある場合
	if f.read():
		f = open(filePath, 'r')
		jsonData = json.load(f)

		newJsonData = copy.deepcopy(jsonData)
		
		#古くなった不要なデータを削除(12個前のデータ)
		if len(newJsonData['timestamp']) >= 12:
			minTime = min(newJsonData['timestamp'])
			newJsonData['timestamp'].remove(minTime)
			
			for term, dataDic in jsonData['terms'].items():
				newJsonData['terms'][term].pop(minTime)
	
		newJsonData['timestamp'].append(strTime)

		#元のjsonDataにある単語について処理を行う．
		#その際最新のトレンドに入っている単語については最新の数値を，
		#入っていない単語については[0, 'Nan']を代入
		for term, dataDic in jsonData['terms'].items():
			if term in trendDic:
				i = 0
				cooccur = ""
				for cooccurWord, count in sorted(cooccurTerms[term].items(), key=lambda x:x[1], reverse=True):
					if i < 3:
						cooccur += "%s  " % cooccurWord
					else:
						break
					
					i += 1
						
				weight = round(trendDic[term], 5)
				newJsonData['terms'][term][strTime] = [weight, cooccur]
				
			else:
				newJsonData['terms'][term][strTime] = [0, 'Nan']
				
		#最新のトレンドにはあるが，元のjsonDataにはない単語について処理を行う．
		for term, weight in trendDic.items():
			if term not in jsonData['terms']:
				tmpDic = {}
				for i in range(len(newJsonData['timestamp'])-1):
					tmpDic[newJsonData['timestamp'][i]] = [0, 'Nan']

				i = 0
				cooccur = ""
				for cooccurWord, count in sorted(cooccurTerms[term].items(), key=lambda x:x[1], reverse=True):
					if i < 3:
						cooccur += "%s  " % cooccurWord
					else:
						break
					
					i += 1
					
				weight = round(weight, 5)
				tmpDic[strTime] = [weight, cooccur]
				
				newJsonData['terms'][term] = tmpDic

		#0より大きい値をもたない，不要な単語を除去
		tmpJsonData = copy.deepcopy(newJsonData)
		for term, dataDic in tmpJsonData['terms'].items():
			flag = False
			
			for time, eachList in dataDic.items():
				if eachList[0] > 0:
					flag = True
					break
			
			if flag == False:
				newJsonData['terms'].pop(term)
				
	#filePathのjsonにデータがない場合
	else:
		newJsonData['timestamp'] = []
		newJsonData['terms'] = {}
		
		newJsonData['timestamp'].append(strTime)

		#トレンドにある単語の情報を辞書へ追加
		for term, weight in trendDic.items():
			newJsonData['terms'][term] = {}
			
			i = 0
			cooccur = ""
			for cooccurWord, count in sorted(cooccurTerms[term].items(), key=lambda x:x[1], reverse=True):
				if i < 3:
					cooccur += "%s  " % cooccurWord
				else:
					break
				
				i += 1
				
			weight = round(weight, 5)
			newJsonData['terms'][term] = {strTime:[weight, cooccur]}

	f.close()

	f = open(filePath, 'w')
	json.dump(newJsonData, f, indent=4)

	f.close()

