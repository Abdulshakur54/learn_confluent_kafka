#!/usr/bin/env python
from random import choice
from confluent_kafka import Producer


if __name__ == '__main__':
    customers = ['Abdulshakur', 'Bola', 'Jimoh', 'Suleman', 'Paul', 'Jimmy', 'Sola', 'Musa', 'Kate']
    products = ['Bag', 'Wrist Watch', 'Mobile Phone', 'T Shirt', 'Battery', 'Laptop', 'Cup', 'Plates', 'Shoes', 'Calculator']
    quantities = [1, 2, 3, 4, 5, 6, 7]

    config = {
        # User-specific properties that you must set
        'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
        'sasl.username':     'CGAIWYZIP67CTR2V',
        'sasl.password':     'bZ7np20TKVaOn1R9wDfaD7BxFrz3Aah4olNqadBe1MSVBpJlRLcgOdyeGAW0226h',

        # Fixed properties
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        'acks':              'all'
    }

    producer = Producer(config)

    def producer_callback(err, msg):
        if err:
            print(f'Error: {err}')
        else:
            print('******************')
            print('Message Produced')
            print('******************')
            print(f'Topic: {msg.topic()}')
            print(f"Message Key: {msg.key().decode('utf-8')}")
            print(f"Message Value: {msg.value().decode('utf-8')}")
    
    topic = 'purchases'

    for count in range(5):
        customer = choice(customers)
        product = choice(products)
        quantity = choice(quantities)
        message = f'Bought {quantity} {product}'
        producer.produce(topic, message, customer, callback = producer_callback)
    
    print('Before calling poll expecting to wait for 60 seconds')
    producer.poll(60)
    producer.flush()

    print('End of calling poll')