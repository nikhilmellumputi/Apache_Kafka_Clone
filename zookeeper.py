#!/usr/bin/env python
import pika
import time
#import schedule
import os



connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
a = channel.queue_declare(queue='Zookeeper_queue')
stat = ""
def callback_zookeeper(ch,method, properties, body):
	global stat
	print(body.decode())
	stat = body.decode()
	if stat.upper() == "NOT HEALTHY":
		print("broker has stopped working properly")
		exit(0)
	
channel.basic_consume(queue='Zookeeper_queue', on_message_callback= callback_zookeeper, auto_ack= True)

print("checking the status of the broker")
channel.start_consuming()
