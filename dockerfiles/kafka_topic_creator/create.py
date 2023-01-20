import confluent_kafka.admin, pprint

conf        = {'bootstrap.servers': 'kafka:9092'}
kafka_admin = confluent_kafka.admin.AdminClient(conf)

new_topic   = confluent_kafka.admin.NewTopic('test', 4, 1)
                  # Number-of-partitions  = 1
                                    # Number-of-replicas    = 1
kafka_admin.create_topics([new_topic,]) # CREATE (a list(), so you can create multiple).
pprint.pprint(kafka_admin.list_topics().topics) # LIST
