#!/usr/bin/env python
import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
topic_names = []
f = open("topics/topics_names.txt", 'r')
f_names = f.readlines()
print("the existing topics are:")
for i in f_names:
	a= i[:-1]
	if(a not in topic_names):
		topic_names.append(a)
	print(a)
channel.queue_declare(queue='Broker_Publisher_topic')
channel.queue_declare(queue='Broker_Publisher')
x = 1
while(x):
	inp = input("do you want to send a message?(y/n)")
	if inp == 'y' or inp == 'Y':
		inp_topic = input("which topic do you want to update/publlish to?")
		inp_message = input("message:")
		#channel.basic_publish(exchange='', routing_key='hello', body= "first queue")
		if(inp_topic not in topic_names):
			topic_names.append(inp_topic)
			f = open("topics/topics_names.txt",'a+')
			f.write(inp_topic + "\n")
			f.close()
		channel.basic_publish(exchange='', routing_key='Broker_Publisher_topic', body= inp_topic)
		channel.basic_publish(exchange='', routing_key='Broker_Publisher', body= inp_message)
		print(f" [x] Sent {inp_message}")
	else : 
		print(f" [x] closing connection")
		connection.close()
		x = 0
