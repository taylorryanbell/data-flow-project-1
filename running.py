# Dataflow Pipeline Project 1 (v2)
# Author: Taylor Bell
# 2021.12.31
# For York Solutions B2E Program
# Description: Access a Google Cloud Pub/Sub subscription to retrieve message data on customer orders, then transform
#       the orders to match a given specification for three BigQuery tables. Filter all orders by currency, and then
#       append each as a row to the correct BigQuery table (Creating the tables if needed). Finally, post a message
#       to a different Pub/Sub topic which contains the order_id and a list of all order_item ids for each order, in
#       order to provide a stock update for inventory purposes.


# import required packages
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery

# Setting constant values
SUBSCRIPTION_ID = 'projects/york-cdf-start/subscriptions/trb_order_data-sub'
PROJECT_ID = 'york-cdf-start'
DATASET_ID = 'taylorryanbell_4'
POSTING_TOPIC_ID = 'projects/york-cdf-start/topics/dataflow-order-stock-update'
USD_TABLE_ID = 'usd_order_payment_history'
EUR_TABLE_ID = 'eur_order_payment_history'
GBP_TABLE_ID = 'gbp_order_payment_history'


# currency filters (for use with apache beam's Filter() transform)
def isUSD(element):
    return element['order_currency'] == 'USD'


def isEUR(element):
    return element['order_currency'] == 'EUR'


def isGBP(element):
    return element['order_currency'] == 'GBP'


# transforms
class ToDictionary(beam.DoFn):
    def process(self, element):

        # decode and load the json from bytestring into dictionary
        element = json.loads(element.decode("UTF-8"))
        yield element


class NameSplitter(beam.DoFn):
    def process(self, element):

        # split customer_name into first and last names
        element["customer_first_name"] = element["customer_name"].split()[0]
        element["customer_last_name"] = element["customer_name"].split()[1]

        yield element


class AddressSplitter(beam.DoFn):
    def process(self, element):

        # for Residential Addresses
        if len(element["order_address"].split(', ')) == 3:

            # set building number
            if (element["order_address"].split(', ')[0]).split(' ', 1)[0].isnumeric():
                order_building_number = element["order_address"].split(', ')[0].split(' ', 1)[0]
            else:
                order_building_number = None

            # set street name
            order_street_name = element["order_address"].split(', ')[0].split(' ', 1)[1]

            # set city
            order_city = element["order_address"].split(', ')[1]

            # set state code
            order_state_code = element["order_address"].split(', ')[2].split(' ')[0]

            # set zip code
            order_zip_code = element["order_address"].split(', ')[2].split(' ')[1]

        # For military addresses
        else:

            # set building number
            if element["order_address"].split(', ')[0].split(' ', 1)[0].isnumeric():
                order_building_number = element["order_address"].split(', ')[0].split(' ', 1)[0]
            else:
                order_building_number = None

            # set street name
            order_street_name = element["order_address"].split(', ')[0].split(' ', 1)[1]

            # set city
            order_city = element["order_address"].split(', ')[-1].split(' ')[0]

            # set state code
            order_state_code = element["order_address"].split(', ')[-1].split(' ')[1]

            # set zip code
            order_zip_code = element["order_address"].split(', ')[-1].split(' ')[2]

        address_dict = {
            'order_building_number': order_building_number,
            'order_street_name': order_street_name,
            'order_city': order_city,
            'order_state_code': order_state_code,
            'order_zip_code': order_zip_code
        }

        element['order_address'] = [address_dict]

        yield element


class Pricer(beam.DoFn):
    def process(self, element):

        # create an empty running total count
        total = 0

        # iterate through order_items and sum the price for each
        for item in element['order_items']:
            total += round(float(item['price']), 2)

        # add shipping and tax to the total
        total += round(float(element['cost_shipping']) + float(element['cost_tax']), 2)

        # set the total into the order dictionary
        element['cost_total'] = total

        yield element


class FilterData(beam.DoFn):
    def process(self, element):

        # create a new dictionary with only the required columns, leaving the rest behind
        output_dict = {
            'order_id': element["order_id"],
            'order_address': element["order_address"],
            'customer_first_name': element["customer_first_name"],
            'customer_last_name': element["customer_last_name"],
            'customer_ip': element["customer_ip"],
            'cost_total': element["cost_total"]
        }

        yield output_dict


class OrderMessage(beam.DoFn):
    def process(self, element):

        # create order message dictionary
        message_dict = {
            'order_id': element['order_id'],
            'order_items': []  # blank list to be populated by order_ids
        }

        # iterate through order items and grab id for each
        for item in element['order_items']:
            message_dict['order_items'].append(int(item['id']))

        # re-encode dict as bytes for Pub/Sub
        message_bytes = json.dumps(message_dict).encode('UTF-8')

        yield message_bytes


# class TransformAll(beam.DoFn):  # perform necessary transformations to clean the data, outputs a dictionary
#     def process(self, element):
#         element_dict = {}  # creates a blank dictionary
#         output = element.decode("utf-8")  # convert byte string to JSON string
#         full_order = json.loads(output)  # load JSON string as a Python Dictionary
#
#         # get order_id
#         element_dict['order_id'] = int(full_order['order_id'])
#
#         # address transform
#         element_dict['order_address'] = []  # create blank list for order_address
#         address_dictionary = {}  # create blank dictionary to fill with specific order_address details
#         address_string = full_order['order_address']  # import order_address only, as a string
#         address_list = address_string.split(', ')  # splits order_address at the comma, for further evaluation
#         if len(address_list) == 3:  # checks if address had 2 commas (residential)
#             street_address = address_list[0].split(' ', 1)  # split street address into number and name
#             order_building_number = street_address[0]
#             order_street_name = street_address[1]
#             order_city = address_list[1]
#             state = address_list[2].split(' ', 1)
#             order_state_code = state[0]
#             order_zip_code = state[1]
#
#             # set
#             address_dictionary["order_building_number"] = order_building_number
#             address_dictionary["order_street_name"] = order_street_name
#             address_dictionary["order_city"] = order_city
#             address_dictionary["order_state_code"] = order_state_code
#             address_dictionary["order_zip_code"] = int(order_zip_code)
#         elif len(address_list) == 2:  # checks if address had 1 comma (military)
#             mil_part_1 = address_list[0].split(' ')
#             mil_part_2 = address_list[1].split(' ')
#             address_dictionary["order_building_number"] = ''
#             address_dictionary["order_street_name"] = mil_part_1[1]
#             address_dictionary["order_city"] = ''
#             address_dictionary["order_state_code"] = mil_part_2[0]
#             address_dictionary["order_zip_code"] = mil_part_2[1]
#
#         element_dict["order_address"] = [address_dictionary]
#         # print(element_dict['order_address'])
#
#         # name transform
#         name_string = full_order['customer_name']
#         name_list = name_string.split(' ')
#         first_name = name_list[0]
#         last_name = name_list[1]
#         # print(first_name)
#         # print(last_name)
#         element_dict['customer_first_name'] = first_name
#         element_dict['customer_last_name'] = last_name
#
#         # get customer_ip
#         element_dict['customer_ip'] = full_order['customer_ip']
#
#         # pricer transform combination
#         running_total = 0.0
#         running_total += float(full_order['cost_shipping'])
#         running_total += float(full_order['cost_tax'])
#
#         for item in full_order['order_items']:
#             running_total += float(item['price'])
#
#         element_dict["cost_total"] = running_total
#         # print(running_total)
#
#         # get order_currency
#         element_dict["order_currency"] = full_order['order_currency']
#
#         # print("Sending " + str(element_dict) + "to BigQuery")
#         yield element_dict


def run():

    opt = PipelineOptions(project="york-cdf-start",
                          region="us-central1",
                          temp_location="gs://york-project-bucket/taylorryanbell/dataflow/tmp/",
                          staging_location="gs://york-project-bucket/taylorryanbell/dataflow/staging/",
                          streaming=True,
                          save_main_session=True)

    table_schema = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'order_address', 'type': 'RECORD', 'mode': 'REPEATED', 'fields': [
                {'name': 'order_building_number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'order_street_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'order_city', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'order_state_code', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'order_zip_code', 'type': 'INTEGER', 'mode': 'NULLABLE'}
            ]},
            {'name': 'customer_first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'customer_last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'customer_ip', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'cost_total', 'type': 'FLOAT', 'mode': 'NULLABLE'}
        ]
    }

    table1 = bigquery.TableReference(
        projectId=PROJECT_ID,
        datasetId=DATASET_ID,
        tableId=USD_TABLE_ID)
    table2 = bigquery.TableReference(
        projectId=PROJECT_ID,
        datasetId=DATASET_ID,
        tableId=EUR_TABLE_ID)
    table3 = bigquery.TableReference(
        projectId=PROJECT_ID,
        datasetId=DATASET_ID,
        tableId=GBP_TABLE_ID)

    with beam.Pipeline(runner="DataflowRunner", options=opt) as pipeline:

        # pull in data
        data = pipeline | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION_ID)

        # convert to dictionary
        order = data | 'ConvertToDictionary' >> beam.ParDo(ToDictionary())

        # grab order message order_id and order_items list
        order_message = order | beam.ParDo(OrderMessage())
        order_message | beam.io.WriteToPubSub(topic=POSTING_TOPIC_ID)

        # apply transforms
        split_name = order | 'SplitName' >> beam.ParDo(NameSplitter())
        split_address = split_name | 'SplitAddress' >> beam.ParDo(AddressSplitter())
        priced = split_address | 'Priced' >> beam.ParDo(Pricer())

        # filter by currency
        usd_filter = priced | "USD Filter" >> beam.Filter(isUSD)
        eur_filter = priced | "EUR Filter" >> beam.Filter(isEUR)
        gbp_filter = priced | "GBP Filter" >> beam.Filter(isGBP)

        # filter to only necessary columns
        usd_orders = usd_filter | "USD Orders" >> beam.ParDo(FilterData())
        eur_orders = eur_filter | "EUR Orders" >> beam.ParDo(FilterData())
        gbp_orders = gbp_filter | "GBP Orders" >> beam.ParDo(FilterData())

        # create table for usd
        usd_orders | "WriteTable_USD" >> beam.io.WriteToBigQuery(
            table1,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # create table for eur
        eur_orders | "WriteTable_EUR" >> beam.io.WriteToBigQuery(
            table2,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # create table for gbp
        gbp_orders | "WriteTable_GBP" >> beam.io.WriteToBigQuery(
            table3,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        pass


if __name__ == '__main__':
    print('+---------------------------+')
    print('| Dataflow Project 1 v2.0.1 |')
    print('+---------------------------+')
    # logging.getLogger().setLevel(logging.INFO)
    run()
