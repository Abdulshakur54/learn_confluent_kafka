#!/usr/bin/env python
from confluent_kafka import Consumer

if __name__ == '__main__':
    topics = ['purchases', "poem", "poem_1", "poem_4"]
    config = {
        # User-specific properties that you must set
        'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
        'sasl.username':     'CGAIWYZIP67CTR2V',
        'sasl.password':     'bZ7np20TKVaOn1R9wDfaD7BxFrz3Aah4olNqadBe1MSVBpJlRLcgOdyeGAW0226h',

        # Fixed properties
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        'group.id':          'kafka-python-getting-started',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(config)
    consumer.subscribe(topics)

    try:
        while True:
            msg = consumer.poll(1)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print(f"Error: {msg.error()}")
            else:
                print({
                    "topic": msg.topic(),
                    "key": msg.key().decode('utf-8'),
                    "value": msg.value().decode('utf-8')
                })
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        


