#!/usr/bin/env python
import pika, sys, os
import random
#import schedule
topic_name = ""
topic_list =[]
def main():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()

	channel.queue_declare(queue='Broker_Publisher_topic')
	channel.queue_declare(queue='Broker_Publisher')
	channel.queue_declare(queue='Broker_Consumer')
	channel.queue_declare(queue='Broker_consumer_message')
	channel.queue_declare(queue='Broker_consumer_length')
	channel.queue_declare(queue='Zookeeper_queue')
	
	#Zookeeper Part:
	channel.basic_publish(exchange='',routing_key='Zookeeper_queue',body='Healthy')
	
	global topic_list
	f = open("topics/topics_names.txt", 'r')
	f_names = f.readlines()
	#print("the existing topics are:")
	for i in f_names:
		if(i not in topic_list):
			topic_list.append(i)
	#Publisher Part
	def callback_topic(ch, method, properties, body):
		global topic_name
		topic_name = "topics/"+ body.decode() + ".txt"
		print(topic_name)
		channel.basic_consume(queue='Broker_Publisher', on_message_callback=callback_message, auto_ack=True)
		channel.basic_publish(exchange='',routing_key='Zookeeper_queue',body='Healthy')
	
	def callback_message(ch, method, properties, body):
		f= open(topic_name, "a+")
		msg = body.decode()
		randomint =random.randint(1,100)
		if(len(msg)>30):
			msg2 = msg[0:30]+'\t'+ str(randomint)
		f.write(msg2+"\n")
		print("the topic is updated")
		f.close()
		f = open("topics/partition.txt","a+")
		if(len(msg)>30):
			f.write(msg[30:] + '\t' + str(randomint)+'\n')
		f.close()
		f =open("log.txt","a+")
		f.write(topic_name+'\t'+body.decode()+'\t'+"Publisher\n")
		f.close()
		channel.basic_publish(exchange='',routing_key='Zookeeper_queue',body='Healthy')
	
	channel.basic_consume(queue='Broker_Publisher_topic', on_message_callback=callback_topic, auto_ack=True)
	channel.basic_publish(exchange='',routing_key='Zookeeper_queue',body='Healthy')
	
	print(' [*] Waiting for messages. To exit press CTRL+C')
	
	#Consumer Part
	def callback_consumer_topic(ch, method, properties, body):
		topic = body.decode()
		print(topic+" initialized" )
		if(topic not in topic_list):
			topic_list.append(topic)
			f = open(("topics/"+topic+ ".txt"), "a+")
			f.close()
		topic2 = "topics/"+ body.decode() + ".txt"
		f = open(topic2, "r")
		msg = f.readlines()
		channel.basic_publish(exchange='',routing_key='Zookeeper_queue',body='Healthy')
		#length_msg = str(len(msg))
		#channel.basic_publish(exchange='',routing_key='Broker_consumer_length',body=length_msg)
		#print("Length Sent")
		
		if(len(msg)==0):
			channel.basic_publish(exchange='',routing_key='Broker_consumer_message',body="New Topic Created")
			print("New topic created")
		else:
			if(len(msg)>30):
				temp = msg.split('\t')

			for i in msg:
				channel.basic_publish(exchange='',routing_key='Broker_consumer_message',body=i[:-1])
				channel.basic_publish(exchange='',routing_key='Zookeeper_queue',body='Healthy')
			print(f"contents of topic {topic} sent")
		f.close()
		f = open("log.txt","a+")
		f.write(topic2+'\tMessage Sent\t'+"Consumer\n")
		f.close()

	channel.basic_publish(exchange='',routing_key='Zookeeper_queue',body='Healthy')
	channel.basic_consume(queue='Broker_Consumer',on_message_callback=callback_consumer_topic, auto_ack=True)
	channel.start_consuming()

	
	
	

if __name__ == '__main__':
	try:
		main()
	except KeyboardInterrupt:
		connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		channel = connection.channel()
		channel.queue_declare(queue='Zookeeper_queue')
		channel.basic_publish(exchange='',routing_key='Zookeeper_queue',body='not Healthy')
		print('Interrupted')
	try:
		sys.exit(0)
	except SystemExit:
		os._exit(0)
