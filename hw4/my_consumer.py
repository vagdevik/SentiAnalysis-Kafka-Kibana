from kafka import KafkaConsumer

from textblob import TextBlob
from elasticsearch import Elasticsearch

# create instance of elasticsearch
es = Elasticsearch(["http://localhost:9200"])

consumer = KafkaConsumer(
	bootstrap_servers='localhost:9092',
	auto_offset_reset='latest',
	group_id='hw4_consumer_test'
	)

consumer.subscribe('elon')

for record in consumer:
	tweet_msg = record.value.decode('UTF-8')
	print(tweet_msg)
	tweet_sentiment = TextBlob(tweet_msg)
	if tweet_sentiment.polarity < 0:
		sentiment = "negative"
	elif tweet_sentiment.polarity == 0:
		sentiment = "neutral"
	else:
		sentiment = "positive"

	print(sentiment)
	es.index(index="sentiment",document={"tweet":tweet_msg,"polarity": tweet_sentiment.polarity,"sentiment": sentiment})
	print("polarity:", tweet_sentiment.polarity)
	print("\n ---- \n")


