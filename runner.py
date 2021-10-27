from time import time
import pandas as pd
import cbpro
import csv

from auth_cred import (api_secret, api_key, api_pass)



class TextWebsocketClient(cbpro.WebsocketClient):
    def on_open(self):
        self.url   = 'wss://ws-feed-public.sandbox.pro.coinbase.com'
        self.message_count = 0
        fieldnames = ["x_value", "total_1"]
        with open('data.csv', 'w') as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                csv_writer.writeheader()

    def on_message(self, msg):
        self.message_count +=1
        msg_type = msg.get('type', None)
        if msg_type =='ticker':
            rowcount = 1
            time_val  = msg.get('time',None)
            time_val = pd.Timestamp(time_val).timestamp()
            # df['time'] = pd.to_datetime(df)
            # df['minute'] = df['time'].dt.minute
            # df['second'] = df['time'].dt.second
            # time_val = df['minute'] * 60 + df['second']
            # time_val = pd.to_datetime(df['time']).astype(int)/ 10**9
            price_val  = msg.get('price',None)
            price_val  = float(price_val) if price_val is not None else 'None'
            product_id = msg.get('product_id', None)
            fieldnames = ["x_value", "total_1"]



            with open('data.csv', 'a') as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                info = {
                    "x_value": time_val,
                    "total_1": price_val,
                
                }

                csv_writer.writerow(info)
            print(f"{time_val} {price_val} {product_id}\tchannel type:{msg_type}") 


    def on_close(self):
            print(f"<---Websocket connection closed--->\n\tTotal messages: {self.message_count}")


stream = TextWebsocketClient(products = ['BTC-USD'], channels=['ticker'])
stream.start()
