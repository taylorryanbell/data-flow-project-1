import json
import time
import numpy
import names
import random
from faker import Faker
from datetime import datetime
from google.cloud import pubsub_v1

# pub/sub values
PROJECT_ID="york-cdf-start"
TOPIC = "dataflow-project-orders"

# range values
order_frequency_range = {'min': 1, 'max': 5, 'step': 1}
product_count_range = {'min': 1, 'max': 5, 'step': 1}
product_price_range = {'min': 50, 'max': 5000, 'step': 0.01}
order_tax_range = {'min': 5, 'max': 50, 'step': 0.01}
order_shipping_range = {'min': 5, 'max': 50, 'step': 0.01}

# init
fake = Faker()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)

def generate_order():
    customer_name = names.get_full_name()
    customer_time = datetime.now().strftime('%d/%b/%Y:%H:%M:%S')
    customer_ip = fake.ipv4()

    order_id = fake.postcode()
    order_currency = random.choice(["USD", "EUR", "GBP"])
    order_address = fake.address().replace('\n', ', ')
    order_items = generate_customer_cart()

    cost_shipping = random.choice(
        numpy.arange(
            order_shipping_range['min'],
            order_shipping_range['max'],
            order_shipping_range['step']
        )
    )
    cost_tax = random.choice(
        numpy.arange(
            order_tax_range['min'],
            order_tax_range['max'],
            order_tax_range['step']
        )
    )

    return {
        'customer_name': customer_name,
        'customer_time': customer_time,
        'customer_ip': customer_ip,

        'order_id': order_id,
        'order_currency': order_currency,
        'order_address': order_address,
        'order_items': order_items,

        'cost_shipping': cost_shipping,
        'cost_tax': cost_tax
    }

def generate_customer_cart():
    product_list = []
    product_count = random.choice(
        range(
            product_count_range['min'],
            product_count_range['max'],
            product_count_range['step']
        )
    )

    # generate customer cart
    for i in range(product_count):
        product_list.append({
            'id': fake.postcode(),
            'name': names.get_last_name(),
            'price': round(
                random.choice(
                    numpy.arange(
                        product_price_range['min'],
                        product_price_range['max'],
                        product_price_range['step']
                    )
                ),
                2
            )
        })

    return product_list

def publish(publisher, topic, message):
    data = json.dumps(message)
    return publisher.publish(topic_path, data=data)

def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic_path, message_future.exception()))
    else:
        print(message_future.result())

if __name__ == '__main__':
    while True:
        order = generate_order()
        message_future = publish(publisher, topic_path, order)
        message_future.add_done_callback(callback)

        timeout_seconds = random.choice(
            range(
                order_frequency_range['min'],
                order_frequency_range['max'],
                order_frequency_range['step']
            )
        )

        print(generate_order())
        time.sleep(timeout_seconds)
