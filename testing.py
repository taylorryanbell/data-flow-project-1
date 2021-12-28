import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import time
import json
import numpy
import names
import random
from faker import Faker
from datetime import datetime
from google.cloud import pubsub_v1

# pub/sub values
PROJECT_ID = "york-cdf-start"
TOPIC = "dataflow-project-orders"

# range values
order_frequency_range = {'min': 1, 'max': 5, 'step': 1}
product_count_range = {'min': 1, 'max': 5, 'step': 1}
product_price_range = {'min': 50, 'max': 5000, 'step': 0.01}
cost_tax_range = {'min': 5, 'max': 50, 'step': 0.01}
cost_shipping_range = {'min': 5, 'max': 50, 'step': 0.01}

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

    cost_shipping = round(
        random.choice(
            numpy.arange(
                cost_shipping_range['min'],
                cost_shipping_range['max'],
                cost_shipping_range['step']
            )
        ),
        2
    )
    cost_tax = round(
        random.choice(
            numpy.arange(
                cost_tax_range['min'],
                cost_tax_range['max'],
                cost_tax_range['step']
            )
        ),
        2
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
    data = json.dumps(message).encode('utf-8')
    return publisher.publish(topic_path, data=data)


def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(message_future.exception()))
    else:
        print(message_future.result())


class AddressSplitter(beam.DoFn):
    def process(self, element):
        print(element)
        if element[0] == 'order_address':
            address_list = element[1].split(', ')
            street_address = address_list[0].split(' ', 1)
            bld_num = street_address[0]
            street = street_address[1]
            city = address_list[1]
            state = address_list[2].split(' ', 1)
            state_abbrev = state[0]
            zip = state[1]
            yield bld_num, street, city, state_abbrev, zip

class NameSplitter(beam.DoFn):
    def process(self, element):
        if element[0] == 'customer_name':
            full_name = element[1].split()
            first_name = full_name[0]
            last_name = full_name[1]
            # print(first_name)
            # print(last_name)
            yield first_name, last_name

class OrderPricer(beam.DoFn):
    def process(self, element):
        if element[0] == 'order_items':
            total_price = 0
            item_list = element[1]
            for item in item_list:
                total_price += item['price']
            yield total_price

class ShippingPricer(beam.DoFn):
    def process(self, element):
        if element[0] == 'cost_shipping':
            yield element[1]

class TaxPricer(beam.DoFn):
    def process(self, element):
        if element[0] == 'cost_tax':
            yield element[1]

if __name__ == '__main__':
    i = 0
    while i < 3:
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


        print(order)
        time.sleep(timeout_seconds)

        with beam.Pipeline() as pipeline:
            full_order = pipeline | beam.Create(order)
            customer_name = full_order | beam.ParDo(NameSplitter())
            output = full_order | beam.Map(print)
            exit()
            # address = full_order | beam.ParDo(AddressSplitter())
            # order_price = full_order | beam.ParDo(OrderPricer())
            # shipping_price = full_order | beam.ParDo(ShippingPricer())
            # tax_price = full_order | beam.ParDo(TaxPricer())
            #
            # merged = ((customer_name, address, order_price, shipping_price, tax_price) | 'Merge PCollections' >> beam.Flatten())
            # output = merged | beam.Map(print)

        i += 1