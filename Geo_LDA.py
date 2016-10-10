##---------------------- NeighbourHood LDA Models
# this program's responsibility is to create a set of LDA-Topics for each neighbournood of a city
# this version is tuned tothe toronto nieghbourhood shapefile available at: http://www1.toronto.ca/wps/portal/contentonly?vgnextoid=04b489fe9c18b210VgnVCM1000003dd60f89RCRD&vgnextchannel=75d6e03bb8d1e310VgnVCM10000071d60f89RCRD
# the basic flow of the program is to get tweets contained in each boundary polygon,
# 	compute an LDA topic model
#	store the results in a new table with geometry, nHood, ID, and top 10 topics
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
import warnings
warnings.filterwarnings("ignore",category=DeprecationWarning)
import psycopg2
from pprint import pprint as pp
import re
from gensim import corpora, models
import time
import sys


class NHoodLDA:
	def __init__(self):
		self.arg1 = sys.argv[1] # starting nhood (for batching)
		self.arg2 = sys.argv[2] # ending nhood (for batching)
		self.arg3 = sys.argv[3] #name of output table
		self.passes = 50

	def computeCorpus(self):
		#DO STUFF
		start = time.time()
		#Connect to postgres
		db_name =''
		db_user = ''
		db_host = ''
		db_host = ''
		db_port = ''
		db_password = ''
		connString = "dbname='"+db_name+"' user='"+db_user+"' host='"+db_host+"' port='"+db_port+"' password='"+db_password+"'"
		# select only three nHoods to keep problem size reasonable for now....
		poly_tableName = "van_nhood_wgs84"
		poly_identCol = "NAME"
		poly_geomCol = "geom"
		poly_selectString = "SELECT "+poly_identCol+", ST_AsEWKT("+poly_geomCol+") FROM "+poly_tableName+" ORDER BY "+poly_identCol
		logging.info("connecting to database")		
		conn = psycopg2.connect(connString) #connect to DB
		logging.info("creating cursor")
		#cursor for reading the nHoods 
		polyCurr = conn.cursor()
		

		#cursor for writing the results of the modelling to the db
		geoLDAResultsCurr = conn.cursor()		
		#create a results table for this run
		date_time = time.strftime("%d_%b_%y_at_%H%M") #timestamp for this table creation / unique name
		# poly_identCol - name
		# topicNum - the topic number for a given hood
		# wieght - the probability of the first term of the topic
		# word1, word2 ... word10 - the words associated witht he current topic
		# resultsStatement = "CREATE TABLE GeoLDA_"+date_time+" ("+poly_identCol+" varchar(255), topicNum int, w1prob float, word1 varchar(255), w2prob float, word2 varchar(255),w3prob float, word3 varchar(255), w4prob float, word4 varchar (255), w5prob float, word5 varchar(255), w6prob float, word6 varchar(255),w7prob float, word7 varchar(255),w8prob float,word8 varchar(255),w9prob float,word9 varchar(255),w10prob float,word10 varchar(255))"
		# modified for argv name
		try:
			resultsStatement = "CREATE TABLE IF NOT EXISTS GeoLDA_"+self.arg3+" ("+poly_identCol+" varchar(255), topicNum int, w1prob float, word1 varchar(255), w2prob float, word2 varchar(255),w3prob float, word3 varchar(255), w4prob float, word4 varchar (255), w5prob float, word5 varchar (255))"
			geoLDAResultsCurr.execute(resultsStatement)
		except psycopg2.Error as e:
			logging.info("error code:"+e.pgcode)
			logging.info("error message: "+e.diag.message_primary)
			logging.info("error info: "+e.pgerror)
			logging.info("table already exists error, continuing anyways...")


		
		#get data from postgres table
		logging.info("executing select statement")
		polyCurr.execute(poly_selectString)


		#loop through the nhoods
		i =0
		for poly in polyCurr:
			if i >=int(self.arg1) and i<int(self.arg2):
				#2. for each nHood get the tweets contained therein
				logging.info("starting analysis for:"+poly[0])
				#make a select statement from the current nHood			
				tweet_table = "twitter" #the table the tweets are stred within
				tweet_tweetCol = "tweet" #the column name the tweet text is in
				tweet_geomCol = "coords" #the geometry (point) column the tweet coordinates are in
				tweetSelect = "SELECT "+tweet_tweetCol+" FROM "+tweet_table+" WHERE "+tweet_geomCol+" && ST_AsEWKT(\'"+poly[1]+"\')"
				#make a cursor to traverse this nHood's tweets
				tweetsCurr = conn.cursor()
				tweetsCurr.execute(tweetSelect)
				logging.info("Tweets receieved. Starting GENSIM code")

				documents=[]
				logging.info("INCOMMING TWEET CORPUS SIZE: "+str(tweetsCurr.rowcount))
				for tweet in tweetsCurr:
					#tweet = str(curr.fetchone()[0])
					documents.append(' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(gt)"," ",tweet[0]).split()).lower())
				#stopwords
				logging.info("CORPUS SIZE AFTER REGEX: "+str(len(documents)))
				stoplist = set("a about above after again against all am an and any are aren\'t as at be because been before being below between both but by can\'t cannot could couldn\'t did didn\'t do does doesn\'t doing don\'t down during each few for from further had hadn\'t has hasn\'t have haven\'t having he he'd he\'ll he\'s her here here\'s hers herself him himself his how how\'s i i\'d i\'ll i\'m i\'ve if in into is  isn\'t it it\'s its itself let\'s me more most mustn't my myself no nor not of off on once only or other ought our ours ourselves out over own same shan\'t she she\'d she\'ll she\'s should shouldn\'t so some something such than that that\'s the their theirs them themselves then there there\'s these they they\'d they\'ll they're they\'ve this those through to too under until up very was wasn\'t we we\'d we\'ll we\'re we\'ve were weren\'t what what\'s when when\'s where where\'s which while who who\'s whom why why\'s with won\'t would wouldn\'t you you\'d you\'ll you\'re you\'ve your yours yourself yourselves a b c d e f g h i j k l m n o p q r s t u v w x y z don que con en de le sus el re ll rt si go can la ve hi ur dis ain es wanna couldn thx je te ese rn tu ya lo como por pm ca amp como me je oye mi del tho un une da los doin yo nah im lt da se su thru vs una mas uno imma didn ni para tira pa las nos esto dm say know like ima just thought tx way whats say get said dem esta going dont get san qu bien even mf yea good seems knew thing except san yay sabes really yes mis soy vaz em wasn xo got goes need never il ah hey doesn vos keep already telling keeps people much think talk will estar cuando telling shouldno ida llevar much talk feel every someone oh haha miss cause ser tiempo now told come back one al watching thank cant back looks great much mean plase seb dormir ser plzz thanks new literally soon take must time try still end join tbt see las right look anything anymore better tag make makes sure start okay aren give hard pretty let finally start many ever na ng ko stop looking seeing actually things ha probably tonight nice today says ready without done everyone nothing tilltell meet coming others next absolutely hoy bye ma made tug yeah enjoy lil late day side piece find shout dude dudes appearently favourite definitely 0 1 2 3 4 5 6 7 8 9 tell find words want met gea leave please guys guy us sounds otherwise big name amazing missing biyi isn happened besides donde via vamos sleep bed morning put hours finna af phil saying amor est mine iight put joe fuck fucking shit stay stand row wear via hours aqui hay monday tuesday wednesday thursday friday saturday sunday remember close long jerry centro last omg lol lmfao rofl place seen early gotta whole ones stand ok wait lmao year trippin hasta messing lame ugh yet wtf idk act bae away anyone bring damn ig pues alright tf might xd wrong starting little maybe gets sometimes known getting whatever later together left gonna else tf anybody nobodyana starting whatever needs casa happiest bout lefttil eso almost everybody till swear yall around excited best wrong follow far annoying pls gonna favorite babe maybe wants".split())
				
				#turn the sentances into a list of words used (toeknize)
				texts = [[word for word in document.lower().split() if word not in stoplist] for document in documents]		
				logging.info("CORPUS SIZE AFTER STOPLIST: "+str(len(texts)))
				#remove words only spoken once
				all_tokens = sum(texts, [])
				logging.info("beginning tokenization")
				tokens_once = set(word for word in set(all_tokens) if all_tokens.count(word) == 1)
				logging.info("words tokenized, starting single mentioned word reduction")
				texts = [[word for word in text if word not in tokens_once] for text in texts]
				logging.info("words mentioned only once removed")
				#get rid of null entries
				texts = filter(None,texts)
				logging.info("CORPUS SIZE AFTER EMPTY ROWS REMOVED: "+str(len(texts)))			
				dictionary = corpora.Dictionary(texts)

				corpus = [dictionary.doc2bow(text) for text in texts]

				tfidf = models.TfidfModel(corpus) #step 1. --initialize(train) a model

				corpus_tfidf = tfidf[corpus] # Apply TFIDF transform to entire corpus

				logging.info("starting LDA model")
				#run the model - consider putting in an alph and beta cooefficient aplha=
				model = models.ldamodel.LdaModel(corpus_tfidf, id2word=dictionary, alpha=0.001, num_topics=10, update_every=0, passes=self.passes)

				end = time.time()
				elapsed = end-start
				print "\n\nProcess completed in %.2f minutes"%(elapsed/60)
				# take a look at results
				print "\n\n\n\t\t\ttopics"
				print "\nSelect Statement: "+poly_selectString
				print "Corpus Size: "+ str(len(texts))
				pp(model.show_topics(topics=10, topn=5, log=False,formatted=True))
				
				# write each topic to the table
				output = model.show_topics(topics=10, topn=5, formatted=False)			
				topicnum=0
				for topic in output:
					topicnum+=1 #iterate this topic
					logging.info("writing results to database. All nHoods, "+str(self.passes)+" passes nhood:"+poly[0]+". topic:"+str(topicnum)) #give some info 
					# write each of the words in the topic to a row                            #name    topicNum probWeight  ,firstword   ,secondword									polyname		topicnumber			w1prob						word1			     w2prob				     word2			      w3prob			    	word3				w4prob				   word4				w5prob        		    word5				w6prob				word6				w7prob			   word7				w8prob				word8				w9prob				word9				w10prob				word10
					geoLDAResultsCurr.execute("INSERT INTO GeoLDA_"+self.arg3+" ("+poly_identCol+",topicNum,w1prob,word1,w2prob,word2,w3prob,word3,w4prob,word4,w5prob,word5) VALUES(\'"+poly[0]+"\',\'"+str(topicnum)+"\',\'"+str(topic[0][0])+"\',\'"+topic[0][1]+"\',\'"+str(topic[1][0])+"\',\'"+topic[1][1]+"\',\'"+str(topic[2][0])+"\',\'"+topic[2][1]+"\',\'"+str(topic[3][0])+"\',\'"+topic[3][1]+"\',\'"+str(topic[4][0])+"\',\'"+topic[4][1]+"\')")
				conn.commit()
			i+=1






def main ():
	nHoodLDA = NHoodLDA()
	nHoodLDA.computeCorpus()


if __name__ == '__main__':
	main()
