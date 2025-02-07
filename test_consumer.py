#!/usr/bin/env python
import pika
import sys
import os

length_of_msg=0
def main():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='Broker_Consumer')
	channel.queue_declare(queue='Broker_consumer_message')
	channel.queue_declare(queue='Broker_consumer_length')
	global length_of_msg
	f = open("topics/topics_names.txt", 'r')
	f_names = f.readlines()
	print("the list of all the topics are:")
	for i in range(0,len(f_names)):
		print((i+1),f_names[i],end = '')
	f.close()
	x=1
	while(x):
		ch = input("do you want to continue or exit?(c/e)")
		if ch == 'c' or ch == 'C':
			TN = int(input("which topic do you want to access?"))
			top_name = f_names[TN-1]
			channel.basic_publish(exchange='', routing_key='Broker_Consumer', body= top_name[:-1])
			#comment part comes here
			print("The topic Messaages are ")
			def callback_consumer_message(ch, method, properties, body):
				print(body.decode())
			channel.basic_consume(queue='Broker_consumer_message',on_message_callback=callback_consumer_message, auto_ack=True)
			f.close()
			channel.start_consuming()
		else:
			print("Thank You")
			connection.close()
			x=0
		
		

if __name__ == '__main__':
	try:
		main()
	except KeyboardInterrupt:
		print('Interrupted')
	try:
		sys.exit(0)
	except SystemExit:
		os._exit(0)
