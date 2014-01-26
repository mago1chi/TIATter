#TIATter

##About TIATter
TIATterはTwitterのトレンド解析器を実装したものです．  
動作させる際はcronを使うことで常時稼動させることを前提としています．  
本プロダクトの基本動作は次のようになります．  
まずtiatter_core_hourly.pyを実行することで，  
TwitterのStreaming APIから日本語のツイートを取得し，取得したツイートから  
単語を抽出する，という動作を繰り返し続けます．  
この処理を１時間続けた後，cronによってcalcResults_hourly.pyを実行します．  
すると，ここ１時間のトレンド語を計算します．計算した結果はjsonファイルとして  
書き出されます．このとき出力先ファイルにはトレンド語だけでなく，その単語がどれだけ  
の度合いでトレンドとなっているかの数値と，その単語と頻繁に共起している単語の  
上位３つが同時に書き出されます．  

##About each component
###dbUpdater.py
ツイートから抽出した単語と，その単語の出現頻度をデータベースへ保存します．  
データベースはPostgreSQLの使用を前提としています．

###fetchStream.py
TwitterのStreaming APIから日本語のツイートを取得します．

###parse.py
入力された文字列を形態素解析するためのプログラムです．

###fileExtractor.py
取得したツイートをparse.pyにより形態素解析し，抽出した単語を  
dbUpdater.pyによりデータベースへ保存します．

###tsTfIdf.py
データベースに保存された単語とその出現数からトレンドを計算します．  

###jsonCreator.py
計算結果となるトレンド語を指定されたファイル名にjson形式で保存します．

