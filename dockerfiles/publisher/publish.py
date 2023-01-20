from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': "kafka:9092"}

producer = Producer(conf)

producer.produce("test", value="value")

def acked(err, msg):
  if err is not None:
    print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
  else:
    print("Message produced: %s" % (str(msg)))

import random, string, time

def randomword():
  length = random.randint(3, 15)
  letters = string.ascii_lowercase
  return ''.join(random.choice(letters) for i in range(length))


while True:
  var1 = randomword()
  var2 = randomword()

  val = {"val1" : var1, "val2": var2}
  
  producer.produce("test", value=str(val), callback=acked)
  producer.flush()
  print("Flushed value: %s"%(str(val)))
  wait_time = random.randint(0, 5)
  if wait_time > 0:
    time.sleep(wait_time)

