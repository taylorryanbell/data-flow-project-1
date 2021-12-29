import json
import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery


class TransformAll(beam.DoFn):  # perform necessary transformations to clean the data, outputs a dictionary
    def process(self, element):
        element_dict = {}  # creates a blank dictionary
        output = element.decode("utf-8")  # convert byte string to JSON string
        full_order = json.loads(output)  # load JSON string as a Python Dictionary

        # get order_id
        element_dict['order_id'] = int(full_order['order_id'])

        # address transform
        element_dict['order_address'] = []  # create blank list for order_address
        address_dictionary = {}  # create blank dictionary to fill with specific order_address details
        address_string = full_order['order_address']  # import order_address only, as a string
        address_list = address_string.split(', ')  # splits order_address at the comma, for further evaluation
        if len(address_list) == 3:  # checks if address had 2 commas (residential)
            street_address = address_list[0].split(' ', 1)  # split street address into number and name
            order_building_number = street_address[0]
            order_street_name = street_address[1]
            order_city = address_list[1]
            state = address_list[2].split(' ', 1)
            order_state_code = state[0]
            order_zip_code = state[1]
            address_dictionary["order_building_number"] = order_building_number
            address_dictionary["order_street_name"] = order_street_name
            address_dictionary["order_city"] = order_city
            address_dictionary["order_state_code"] = order_state_code
            address_dictionary["order_zip_code"] = order_zip_code
        elif len(address_list) == 2:  # checks if address had 1 comma (military)
            mil_part_1 = address_list[0].split(' ')
            mil_part_2 = address_list[1].split(' ')
            address_dictionary["order_building_number"] = ''
            address_dictionary["order_street_name"] = mil_part_1[1]
            address_dictionary["order_city"] = ''
            address_dictionary["order_state_code"] = mil_part_2[0]
            address_dictionary["order_zip_code"] = mil_part_2[1]

        element_dict["order_address"] = [address_dictionary]
        # print(element_dict['order_address'])

        # name transform
        name_string = full_order['customer_name']
        name_list = name_string.split(' ')
        first_name = name_list[0]
        last_name = name_list[1]
        # print(first_name)
        # print(last_name)
        element_dict['customer_first_name'] = first_name
        element_dict['customer_last_name'] = last_name

        # get customer_ip
        element_dict['customer_ip'] = full_order['customer_ip']

        # pricer transform combination
        running_total = 0.0
        running_total += float(full_order['cost_shipping'])
        running_total += float(full_order['cost_tax'])

        for item in full_order['order_items']:
            running_total += float(item['price'])

        element_dict["cost_total"] = running_total
        # print(running_total)

        # get order_currency
        element_dict["order_currency"] = full_order['order_currency']

        # print("Sending " + str(element_dict) + "to BigQuery")
        yield element_dict


def isUSD(element):
    return element["order_currency"] == 'USD'


def isEUR(element):
    return element["order_currency"] == 'EUR'


def isGBP(element):
    return element["order_currency"] == 'GBP'


class FilterData(beam.DoFn):
    def process(self, element):
        output_dict = {}
        output_dict["order_id"] = element["order_id"]
        output_dict["order_address"] = element["order_address"]
        output_dict["customer_first_name"] = element["customer_first_name"]
        output_dict["customer_last_name"] = element["customer_last_name"]
        output_dict["customer_ip"] = element["customer_ip"]
        output_dict["cost_total"] = element["cost_total"]
        yield output_dict


class OrderMessage(beam.DoFn):
    def process(self, element):
        output = element.decode("utf-8")
        full_order = json.loads(output)
        message_dict = {}
        message_dict["order_id"] = full_order["order_id"]
        order_items_list = []

        for item in full_order["order_items"]:
            order_items_list.append(int(item["id"]))

        message_dict["order_items"] = order_items_list  # add items ID list to message_dict
        message_bytes = json.dumps(message_dict).encode("utf-8")  # re-encode dict as bytes for Pub/Sub
        # print("Sending " + str(message_bytes) + "to Pub/Sub")
        yield message_bytes


def run(argv=None, save_main_session=True):
    # parser = argparse.ArgumentParser()
    # known_args, pipeline_args = parser.parse_known_args(argv)
    #
    # pipeline_options = PipelineOptions(pipeline_args)
    # pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    table_schema = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'order_address', 'type': 'RECORD', 'mode': 'REPEATED', 'fields': [
                {'name': 'order_building_number', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'order_street_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'order_city', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'order_state_code', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'order_zip_code', 'type': 'STRING', 'mode': 'NULLABLE'}
            ],
             },
            {'name': 'customer_first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'customer_last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'customer_ip', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'cost_total', 'type': 'FLOAT', 'mode': 'NULLABLE'}
        ]
    }

    table1 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='taylorryanbell_3',
        tableId='usd_order_payment_history')
    table2 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='taylorryanbell_3',
        tableId='eur_order_payment_history')
    table3 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='taylorryanbell_3',
        tableId='gbp_order_payment_history')

    with beam.Pipeline(options=PipelineOptions(streaming=True, save_main_session=True)) as pipeline:

        # pull in data
        data = pipeline | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
            subscription='projects/york-cdf-start/subscriptions/trb_order_data-sub')

        # grab order message order_id and order_items list
        order_message = data | beam.ParDo(OrderMessage())
        pub_sub_writer = order_message | beam.io.WriteToPubSub(
            topic='projects/york-cdf-start/topics/dataflow-order-stock-update')

        # apply transform
        cleaned = data | 'TransformAll' >> beam.ParDo(TransformAll())

        # output = cleaned | "Clean Print" >> beam.Map(print)

        # filter by currency
        usd_filter = cleaned | "USD Filter" >> beam.Filter(isUSD)
        eur_filter = cleaned | "EUR Filter" >> beam.Filter(isEUR)
        gbp_filter = cleaned | "GBP Filter" >> beam.Filter(isGBP)

        # remove currency from dictionary
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
    print('| Dataflow Project 1 v1.0.5 |')
    print('+---------------------------+')
    # logging.getLogger().setLevel(logging.INFO)
    run()
