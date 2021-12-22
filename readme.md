# Project

The company needs up to create a streaming data pipeline in Google Data Flow that will..
  1. Transform the data structure to match our needs
  2. Filter orders into the correct BigQuery table so they get processed correctly
  3. Publish messages to a google Pub/Sub topic to update stock counts


Incoming messages will be a JSON object with following format...
```
{
   "order_id":"46386",
   "order_address":"3857 Veronica Prairie Suite 215, New Charles, WI 06550",
   "order_currency":"EUR",
   
   "customer_ip":"177.216.164.66",
   "customer_name":"Maria Kearney"
   "customer_time":"21/Dec/2021:09:50:31",
   
   "cost_shipping":34.57,
   "cost_tax":12.56,
   
   "order_items":[
      {
         "price":4819.77,
         "id":"23113",
         "name":"Jones"
      },
      {
         "price":3207.65,
         "id":"76518",
         "name":"Yu"
      }
   ],
}
```

## You will need to perform the following steps on each incoming message
  - split the ```customer_name``` field into 
    - ```customer_first_name```
    - ```customer_last_name```



  - split the ```order_address``` field into
    -  ```order_building_number```
    -  ```order_street_name``` (if there is an apartment # or suite # include this here)
    -  ```order_city```
    -  ```order_state_code```
    -  ```order_zip_code```



  - calcuate a ```cost_total``` field from the ```cost_shipping```, ```cost_tax``` and each ```price``` field in the ```order_items``` list



  - publish a ```json``` message for each item in the ```order_items``` field to the ```dataflow-project-orders-stock-update-sub``` subscription with the following format
    ```
    {
      "order_id": 46386,
      "item_list": [23113, 76518]
    }
    ```
    
  - create 3 BigQuery tables ```usd_order_payment_history```, ```eur_order_payment_history```, ```gbp_order_payment_history``` with following structure...
    - order_id: INTEGER
    - order_address: RECORD
      - order_building_number: INTEGER
      - order_street_name: STRING
      - order_city: STRING
      - order_state_code: STRING
      - order_zip_code: INTEGER
    - customer_first_name: STRING
    - customer_last_name: STRING
    - customer_ip: STRING
    - cost_total: FLOAT
    
    
    You will need to place orders into the correct table depending on the ```order_currency``` field
      - ```"order_currency":"USA"``` goes to ```usd_order_payment_history```
      - ```"order_currency":"EUR"``` goes to ```eur_order_payment_history```
      - ```"order_currency":"GBP"``` goes to ```gbp_order_payment_history```
  
  
 
