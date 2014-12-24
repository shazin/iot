# ***********************************
# * Raspberry Pi Consumer for Kafka *
# * Author Shazin Sadakath          *
# **********************************

import RPi.GPIO as GPIO # GPIO Library for Python 
from kafka import KafkaClient, SimpleConsumer # Kafka Python Library https://github.com/mumrah/kafka-python

GPIO.setmode(GPIO.BOARD) 
GPIO.setup(7, GPIO.OUT)

# Change this to point to your Kafka 
kafka = KafkaClient("192.168.0.100:9092")

consumer = SimpleConsumer(kafka, "my-group", "illumination", False)
on = False
for message in consumer:
    # If illumination is less than 300 turn on LED
    if(int(message.message.value) < 300):
	on = True
    else:
	on = False

    GPIO.output(7, on)

# Clean Up
kafka.close()
GPIO.cleanup()
