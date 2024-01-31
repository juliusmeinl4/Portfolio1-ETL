import requests
import json
import base64
import xmltodict
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import subprocess
import shlex
import pytz
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import datetime


with open('./json/projectA-json.json') as f:
    json_credentials = json.load(f)


def get_walmart_token():
    """Retrieve the access token for Walmart API."""
    credentials = json_credentials['WALMART']['credentials']
    encoded_credentials = base64.b64encode(f"{credentials['user']}:{credentials['pass']}".encode()).decode()
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "WM_QOS.CORRELATION_ID": json_credentials['WALMART']['correlationID'],
        "WM_SVC.NAME": json_credentials['WALMART']['walmartServiceName'],
        "Content-Type": "application/x-www-form-urlencoded"
    }
    response = requests.post(json_credentials['WALMART']['clientCredentialEndpoint'], 
                             data={"grant_type": "client_credentials"}, 
                             headers=headers)
    if response.status_code == 200:
        response_data = xmltodict.parse(response.content)
        access_token = response_data['OAuthTokenDTO']['accessToken']
        return access_token
    else:
        raise Exception(f"Error obtaining Walmart token: {response.status_code} {response.text}")

def fetch_walmart_data(token):
    """Fetch orders from Walmart API."""
    date_from = (datetime.datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    endpoint = f"https://marketplace.walmartapis.com/v3/orders?createdStartDate={date_from}"
    headers = {
        "WM_SEC.ACCESS_TOKEN": token,
        "WM_QOS.CORRELATION_ID": json_credentials['WALMART']['correlationID'],
        "WM_SVC.NAME": json_credentials['WALMART']['walmartServiceName'],
        "accept": "application/json"
    }
    response = requests.get(endpoint, headers=headers)
    return response.json() if response.status_code == 200 else None, response.status_code


def process_walmart_data(data):
    """Process and format the Walmart order data."""
    
    orders = pd.json_normalize(data, record_path=['list', 'elements', 'order'])

    
    processed_orders = pd.DataFrame()

    
    for index, row in orders.iterrows():
        
        order_lines = pd.json_normalize(row['orderLines.orderLine'])

        
        order_lines['sku'] = order_lines['item.sku']
        order_lines['qty'] = order_lines['orderLineQuantity.amount'].astype(int)
        order_lines['site'] = 'walmart'

        
        order_lines = order_lines[['sku', 'qty', 'site']]

        
        processed_orders = processed_orders.append(order_lines, ignore_index=True)

    return processed_orders

def walmart_main():
    try:
        token = get_walmart_token()
        api_response, status_code = fetch_walmart_data(token)
        
        if api_response:
            formatted_data = process_walmart_data(api_response)
            total_orders = len(formatted_data)
        else:
            print(f"Error fetching Walmart data. Status Code: {status_code}")
            formatted_data = pd.DataFrame()
            total_orders = 0

        return formatted_data, total_orders, status_code
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame(), 0, None  

def fetch_houzz_data():
    """Fetch orders from Houzz API."""
    houzz_credentials = json_credentials['HOUZZ']
    current_datetime = datetime.datetime.now()
    start_date = current_datetime - timedelta(days=7)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S+00:00")
    
    params = {
        "format": "xml",
        "method": "getOrders",
        "From": start_date_str,
        "To": end_date_str
    }
    
    headers = {
        "X-HOUZZ-API-SSL-TOKEN": houzz_credentials['TOKEN'],
        "X-HOUZZ-API-USER-NAME": houzz_credentials['USER_NAME'],
        "X-HOUZZ-API-APP-NAME": houzz_credentials['APP_ID']
    }

    response = requests.get(houzz_credentials['BASE_URL'], headers=headers, params=params)
    
    return response.text if response.status_code == 200 else None, response.status_code


def parse_xml_to_dataframe(xml_string):
    """Parse XML response to DataFrame."""
    root = ET.fromstring(xml_string)
    rows = []

    for order in root.findall(".//Order"):
        for order_item in order.findall(".//OrderItem"):
            sku = order_item.find("SKU").text
            qty = int(order_item.find("Quantity").text)
            rows.append({"sku": sku, "qty": qty, "site": "Houzz"})

    return pd.DataFrame(rows)

def houzz_main():
    try:
        xml_data, status_code = fetch_houzz_data()
        if xml_data:
            houzz_orders = parse_xml_to_dataframe(xml_data)
            total_orders = len(houzz_orders)
        else:
            houzz_orders = pd.DataFrame()
            total_orders = 0
        return houzz_orders, total_orders, status_code
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame(), 0, None


def fetch_orders():
    """Fetch orders from Faire API."""
    faire_credentials = json_credentials['FAIRE']
    headers = {"X-FAIRE-ACCESS-TOKEN": faire_credentials['API_ACCESS_TOKEN']}
    
    
    seven_days_ago_iso = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S.000Z')


    response = requests.get(faire_credentials['ORDERS_ENDPOINT'], headers=headers, params={'created_at_min': seven_days_ago_iso})

    return response.json() if 200 <= response.status_code <= 299 else None, response.status_code


def orders_to_dataframe(orders_data):
    """Convert fetched orders to DataFrame."""
    rows = []
    if orders_data:
        for order in orders_data['orders']:
            for item in order['items']:
                row = {
                    'sku': item['sku'],
                    'qty': item['quantity'],
                    'site': 'Faire'
                }
                rows.append(row)
    return pd.DataFrame(rows)


def faire_main():
    orders_data, status_code = fetch_orders()
    if orders_data:
        faire_orders = orders_to_dataframe(orders_data)
        total_orders = len(faire_orders)
    else:
        print(f"Error fetching Faire data. Status Code: {status_code}")
        faire_orders = pd.DataFrame()
        total_orders = 0
    return faire_orders, total_orders, status_code



week_ago = datetime.datetime.now() - timedelta(days=7)
filter_by_date = datetime.datetime.now()

def call_curl(curl_command):
    args = shlex.split(curl_command)
    process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return json.loads(stdout.decode('utf-8')), process.returncode

def fetch_woocommerce_data():
    brand1_credentials = json_credentials["Brand1"]
    curl_command = f'''curl {brand1_credentials["url"]}\
    -u {brand1_credentials["credentials"]["user"]}:{brand1_credentials["credentials"]["pass"]}'''
    
    output, return_code = call_curl(curl_command)
    if return_code == 0 and output:  
        return output, 200  
    else:
        return None, return_code 

def process_woocommerce_data(output):
    sales_header = ['sku', 'qty', 'site']
    brand1 = pd.json_normalize(output)

    brand1['date_created'] = pd.to_datetime(brand1['date_created'], errors='coerce')
    brand1 = brand1[(brand1['date_created'] > week_ago) & (brand1['date_created'] < filter_by_date)]
    
    brand1 = brand1.explode('line_items')
    brand1 = pd.json_normalize(brand1['line_items'])

    brand1['site'] = 'Brand1'
    brand1 = brand1.rename({'sku': 'sku', 'quantity': 'qty'}, axis=1)
    brand1 = brand1[sales_header]
    return brand1

def brand1_main():
    output, status_code = fetch_woocommerce_data()
    if output:
        brand1_orders = process_woocommerce_data(output)
        total_orders = len(brand1_orders)
    else:
        print("Error fetching Brand1 data")
        brand1_orders = pd.DataFrame()
        total_orders = 0
    return brand1_orders, total_orders, status_code


def fetch_dsco_data(token, start_date_str, current_datetime):
    """Fetch orders from DSCO API."""
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json", "Accept": "application/json"}
    response = requests.get(json_credentials['DSCO']['BASE_URL'], 
                            params={'ordersCreatedSince': start_date_str, 'until': current_datetime.strftime('%Y-%m-%d')},
                            headers=headers)
    return response.json() if response.status_code == 200 else None, response.status_code


def process_dsco_data(orders, api_name, start_date, current_datetime):
    """Convert fetched orders to DataFrame."""
    sales_header = ['sku', 'qty', 'site']
    
    try:
        df = pd.json_normalize(orders, 'orders')
        
        if not df.empty:
            df['dscoCreateDate'] = pd.to_datetime(df.get('dscoCreateDate'), errors='coerce')

            
            df['dscoCreateDate'] = df['dscoCreateDate'].dt.tz_localize(None)

            df = df[(df['dscoCreateDate'] > start_date) & (df['dscoCreateDate'] < current_datetime)]
            
            df = df.explode('lineItems')
            df = pd.json_normalize(df['lineItems'])

            df['site'] = {
                'nrdtoken': 'Nordstrom',
                'softoken': 'Saks OF 5th',
                'aafestoken': 'Aafes',
                'brandxtoken': 'Bonton',
                'lordtoken': 'Lord & Taylor'
            }[api_name]
            df = df.rename({'sku': 'sku', 'quantity': 'qty'}, axis=1)
            df = df[sales_header]

        return df
    except KeyError:
        print(f"KeyError encountered for {api_name}. Please check the JSON structure.")
        return pd.DataFrame(columns=sales_header)


def dsco_main():
    current_datetime = datetime.datetime.now()  
    start_date = current_datetime - timedelta(days=7)
    start_date_str = start_date.strftime('%Y-%m-%d')
    
    dscosales = pd.DataFrame()
    total_orders = 0
    status_summary = {}

    
    api_tokens = {k: v for k, v in json_credentials['DSCO'].items() if k != 'BASE_URL'}

    for api_name, token in api_tokens.items():
        orders, status_code = fetch_dsco_data(token, start_date_str, current_datetime)
        status_summary[api_name] = status_code

        if orders:
            sales_data = process_dsco_data(orders, api_name, start_date, current_datetime)
            
            dscosales = pd.concat([dscosales, sales_data], ignore_index=True)
            total_orders += len(sales_data)
        else:
            print(f"Error fetching data for {api_name}. Status Code: {status_code}")

    return dscosales, total_orders, status_summary



def fetch_mirakl_data(api_info, start_date_str, end_date_str):
    """Fetch orders from Mirakl API."""
    api_key = api_info['credentials']['user']
    base_uri = f"{api_info['url']}?start_date={start_date_str}&end_date={end_date_str}&max=100"
    headers = {'Authorization': api_key}

    response = requests.get(base_uri, headers=headers)
    return response.json() if response.ok else None, response.status_code

def process_orders(orders, site):
    """Process Mirakl orders."""
    df = pd.json_normalize(orders, 'orders')
    if not df.empty:
        df = df[df['order_state'] != 'CANCELED']
        df = df.explode('order_lines')
        df_final = pd.DataFrame(df['order_lines'].apply(pd.Series))
        read = df_final[['offer_sku', 'quantity']].rename({'offer_sku': 'sku', 'quantity': 'qty'}, axis=1)
        read['site'] = site
        return read
    else:
        return pd.DataFrame(columns=['sku', 'qty', 'site'])

def mirakl_main():
    current_datetime = datetime.datetime.now()  
    start_date = current_datetime - timedelta(days=7)
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_date_str = current_datetime.strftime('%Y-%m-%dT%H:%M:%S')

    mirakl_sold = pd.DataFrame()
    apis_to_process = ['THE BAY', 'VERISHOP', 'SSPO']
    status_summary = {}

    for site in apis_to_process:
        api_info = json_credentials.get(site)
        if api_info:
            orders, status_code = fetch_mirakl_data(api_info, start_date_str, end_date_str)
            status_summary[site] = status_code

            if orders:
                sold = process_orders(orders, site)
                mirakl_sold = mirakl_sold.append(sold, ignore_index=True)
            else:
                logging.error(f'Error occurred for {site}. Status Code: {status_code}')

    return mirakl_sold, status_summary


def get_wayfair_token(json_credentials):
    """Retrieve the access token for Wayfair API."""
    credentials = json_credentials['WAYFAIR']['credentials']
    payload = {
        "grant_type": "client_credentials",
        "client_id": credentials['client_id'],
        "client_secret": credentials['client_secret'],
        "audience": credentials['audience']
    }
    headers = {
        "content-type": "application/json",
        "cache-control": "no-cache"
    }
    response = requests.post(json_credentials['WAYFAIR']['auth_url'], json=payload, headers=headers)
    return response.json()['access_token'] if response.ok else None, response.status_code


def fetch_wayfair_data(token, json_credentials):
    """Fetch data from Wayfair API."""
    
    week_ago_date = datetime.datetime.now() - datetime.timedelta(days=7)
    week_ago_date_str = week_ago_date.strftime('%Y-%m-%dT%H:%M:%S+00:00')  

    query = """
    query getDropshipPurchaseOrders {{
        getDropshipPurchaseOrders(
            limit: 1000,
            hasResponse: false,
            fromDate: "{}",
            sortOrder: DESC
        ) {{
            poNumber,
            poDate,
            products {{
                partNumber,
                quantity
            }}
        }}
    }}
    """.format(week_ago_date_str)

    headers = {"Authorization": "Bearer " + token}
    response = requests.post(json_credentials['WAYFAIR']['api_url'], json={'query': query}, headers=headers)
    
    
    if response.ok:
        return response.json(), response.status_code
    else:
        print("Error in Wayfair API Response:", response.status_code, response.text)
        return None, response.status_code


def process_wayfair_data(data):
    try:
        if 'data' in data and 'getDropshipPurchaseOrders' in data['data']:
            orders = data['data']['getDropshipPurchaseOrders']
            if not orders:
                return pd.DataFrame(), "No orders found"

            all_products = []
            for order in orders:
                for product in order.get('products', []):
                    partNumber = product.get('partNumber', '')
                    quantity = product.get('quantity', 0)  

                    
                    all_products.append({
                        'sku': partNumber,
                        'qty': quantity,
                        'site': 'Wayfair'
                    })

            
            products_df = pd.DataFrame(all_products)
            return products_df, "Success"
        else:
            return pd.DataFrame(), "Expected keys not found in the response"
    except Exception as e:
        return pd.DataFrame(), f"Error processing Wayfair data: {str(e)}"


def wayfair_main():
    token, auth_status = get_wayfair_token(json_credentials)
    if token:
        data, fetch_status = fetch_wayfair_data(token, json_credentials)
        if data:
            wayfair_orders, process_status = process_wayfair_data(data)
            total_orders = len(wayfair_orders)
        else:
            print(f"Error fetching Wayfair data. Status Code: {fetch_status}")
            wayfair_orders = pd.DataFrame()
            total_orders = 0
            process_status = f"Fetch error with status code {fetch_status}"
    else:
        print(f"Error obtaining Wayfair token. Status Code: {auth_status}")
        wayfair_orders = pd.DataFrame()
        total_orders = 0
        fetch_status = auth_status
        process_status = f"Auth error with status code {auth_status}"

    return wayfair_orders, total_orders, fetch_status, process_status



import os

def process_macys_data(PATH, site_name):
    if os.path.isfile(PATH) and os.access(PATH, os.R_OK):
        print(f"File for {site_name} is OKAY")
        data = pd.read_csv(PATH, header=4)
        if 'Insert Date' in data.columns:
            data['Insert Date'] = pd.to_datetime(data['Insert Date'])
        else:
            print("Insert Date column does not exist in the DataFrame.")

        data = data[['Vendor SKU', 'Quantity', 'Merchant']]
        data = data.rename({'Vendor SKU': 'sku', 'Quantity': 'qty', 'Merchant': 'site'}, axis=1)
        total_orders = len(data)
    else:
        print(f"Either the file for {site_name} is missing or not readable")
        data = pd.DataFrame({'sku': ['other'], 'qty': [0], 'site': [site_name]})
        total_orders = 0

    return data, total_orders


macys_data, macys_total_orders = process_macys_data('../sales/macys.csv', 'Macys')


def process_file_data(PATH, site_name, expected_columns, rename_columns):
    if os.path.isfile(PATH) and os.access(PATH, os.R_OK):
        print(f"File for {site_name} is OKAY")
        data = pd.read_excel(PATH) if PATH.endswith('.xls') or PATH.endswith('.xlsx') else pd.read_csv(PATH, sep="\t" if site_name == 'Amazon' else ',')
        data = data[expected_columns]

        
        if 'site' not in rename_columns.values():
            data['site'] = site_name

        data = data.rename(rename_columns, axis=1)
        total_orders = len(data)
    else:
        print(f"Either the file for {site_name} is missing or not readable")
        data = pd.DataFrame({'sku': ['other'], 'qty': [0], 'site': [site_name]})
        total_orders = 0

    return data, total_orders


hsn_data, hsn_total_orders = process_file_data('../sales/hsn.xls', 'HSN', ['Supplier Code', 'QTY', 'RequestorName'], {'Supplier Code' : 'sku', 'QTY': 'qty', 'RequestorName': 'site'})



def process_file_data(PATH, site_name, expected_columns, rename_columns):
    if os.path.isfile(PATH) and os.access(PATH, os.R_OK):
        print(f"File for {site_name} is OKAY")
        data = pd.read_excel(PATH) if PATH.endswith('.xls') or PATH.endswith('.xlsx') else pd.read_csv(PATH, sep="\t" if site_name == 'Amazon' else ',')
        data = data[expected_columns]
        data['site'] = site_name
        data = data.rename(rename_columns, axis=1)
        total_orders = len(data)
    else:
        print(f"Either the file for {site_name} is missing or not readable")
        data = pd.DataFrame({'sku': ['other'], 'qty': [0], 'site': [site_name]})
        total_orders = 0

    return data, total_orders


rue_data, rue_total_orders = process_file_data('../sales/rue.xls', 'Ruelala & Gilt', ['Vendor SKU', 'Quantity'], {'Vendor SKU' : 'sku', 'Quantity': 'qty'})
amazon_data, amazon_total_orders = process_file_data('../sales/amazon.txt', 'Amazon', ['sku', 'quantity'], {'quantity' : 'qty'})
walmart_data, walmart_total_orders = process_file_data('../sales/walmart.xls', 'Walmart', ['SKU', 'Qty'], {'SKU' : 'sku', 'Qty': 'qty'})
tom_data, tom_total_orders = process_file_data('../sales/tom/tom.csv', 'Touch OF Modern', ['Item SKU', 'Qty'], {'Item SKU' : 'sku', 'Qty': 'qty'})


all_sales_data = pd.concat([rue_data, amazon_data, walmart_data, tom_data, macys_data], ignore_index=True)


print(f'Ruelala & Gilt Total Order: {rue_total_orders}')
print(f'Amazon Total Order: {amazon_total_orders}')
print(f'Walmart Total Order: {walmart_total_orders}')
print(f'Touch OF Modern Total Order: {tom_total_orders}')



if __name__ == "__main__":
    
    walmart_data, walmart_total_orders, walmart_status_code = walmart_main()
    print(f'Walmart API Status: {walmart_status_code}, Total Orders: {walmart_total_orders}')

    
    houzz_data, houzz_total_orders, houzz_status_code = houzz_main()
    print(f'Houzz API Status: {houzz_status_code}, Total Orders: {houzz_total_orders}')

    
    faire_data, faire_total_orders, faire_status_code = faire_main()
    print(f'Faire API Status: {faire_status_code}, Total Orders: {faire_total_orders}')

    
    brand1_data, brand1_total_orders, brand1_status_code = brand1_main()
    print(f'Brand1 API Status: {brand1_status_code}, Total Orders: {brand1_total_orders}')

    
    dsco_data, dsco_total_orders, dsco_status_summary = dsco_main()
    print(f'DSCO Total Orders: {dsco_total_orders}')
    for api_name, status_code in dsco_status_summary.items():
        print(f'{api_name} API Status: {status_code}')

    
    mirakl_data, mirakl_status_summary = mirakl_main()
    for site, status_code in mirakl_status_summary.items():
        print(f'{site} API Status: {status_code}')

    
    wayfair_data, wayfair_total_orders, wayfair_status_code, wayfair_process_status = wayfair_main()
    print(f'Wayfair API Status: {wayfair_status_code}, Processing Status: {wayfair_process_status}, Total Orders: {wayfair_total_orders}')


    print("ETL Pipeline execution completed.")



sales = pd.concat([
    hsn_data, 
    rue_data, 
    amazon_data, 
    walmart_data, 
    tom_data, 
    macys_data,
    walmart_data,
    houzz_data,
    faire_data,
    brand1_data,
    dsco_data,
    mirakl_data,
    wayfair_data
], ignore_index=True)


print(sales.head())  
print(f'Total Combined Orders: {len(sales)}')


sales = sales[sales["sku"].str.contains("sku") == False]
sales = sales[sales["sku"].str.contains("Item SKU") == False]
sales = sales[sales["sku"].str.contains("other") == False]
sales = sales.apply(lambda x: x.astype(str).str.lower())
sales['sku'] = sales['sku'].astype('string') 
sales['sku'] = sales['sku'].str.strip()


qtycount = sales[["sku", "qty"]]
qtycount = qtycount[qtycount["sku"].str.contains("sku") == False]
qtycount.set_index('sku')
qtycount['qty'] = qtycount['qty'].astype(float)
qtychanged = qtycount.sort_values(by='sku')
soldvalue = qtychanged.groupby(["sku"]).qty.sum().reset_index()


soldvalue.to_csv("soldvalueretail.csv")


sku_map = pd.read_csv('./skus/skus_map.csv')

combined = pd.merge(soldvalue, sku_map, left_on='sku', right_on='sku_part', how='right')

combined['result'] = combined['qty'] * combined['multiplier']

final_result = combined.groupby('sku_name')['result'].sum().reset_index()
final_result = final_result.rename(columns={'sku_name': 'sku', 'result': 'qty'})
print(final_result)


final_result.to_csv("sold_itemswholesale.csv")

stockathand = pd.read_csv('../cloudbbeh/stockfiles/newstock.csv')

merged_df = pd.merge(stockathand, final_result, on='sku', how='left')

merged_df.fillna(0, inplace=True)

merged_df['new_qty'] = merged_df['qty_x'] - merged_df['qty_y']

final_df = merged_df.drop(columns=['qty_x', 'qty_y']).rename(columns={'new_qty': 'qty'})

column_order = ['sku', 'qty', 'subcategory', 'color', 'brand']

final_df = final_df[column_order]

final_df.to_csv("../cloudbbeh/stockfiles/newstock.csv", index=False)
final_df.to_csv('../cloudbbeh/stock/data/newstock.csv', index = False)
final_df.to_csv('../cloudbbeh/gonder/newstock.csv', index = False)
print(final_df)


final_df = final_df.set_index('sku')

import calendar
from datetime import datetime

sku_map = pd.read_csv('./skus/sales_map.csv')

sales['sku'] = sales['sku'].astype(str)
sku_map['SKU'] = sku_map['SKU'].astype(str)

sales['sku'] = sales['sku'].str.strip()
sku_map['SKU'] = sku_map['SKU'].str.strip()


sales = pd.merge(sales, sku_map, left_on='sku', right_on='SKU', how='left')

sales = sales.drop(columns=['SKU'])

current_date = datetime.now()
sales['date'] = current_date.date()
sales['Year'] = current_date.year
sales['Month'] = calendar.month_name[current_date.month]

sales['date'] = pd.to_datetime(sales['date'])
sales['date_formatted'] = sales['date'].dt.strftime('%m-%d-%Y')

sales["qty"] = pd.to_numeric(sales["qty"], errors='coerce')
sales["cost"] = pd.to_numeric(sales["cost"], errors='coerce')

sales["total"] = sales["qty"] * sales["cost"]

sales = sales.drop(columns=['date_formatted'])


brand1 = sales.loc[sales['brand'] == 'Brand1'] 
brand2 = sales.loc[sales['brand'].isin(['brand2', 'brand3'])]
salesall = [brand1, brand2]
salesall = pd.concat(salesall)
salesall.set_index('sku', inplace=True)


salesall['date'] = pd.to_datetime(salesall['date'], format='%m-%d-%Y')


from datetime import datetime

date_str = datetime.now().strftime('%m-%d-%Y')

path = '../cloudbbeh/eh/2023/data'
excelfilename = f'{date_str}.csv'

output_file = os.path.join(path, excelfilename)

brand1.to_csv(output_file, index=False)


date_str = datetime.now().strftime('%m-%d-%Y')

path = '../cloudbbeh/bb/2023/data'
excelfilename = f'{date_str}.csv'

output_file = os.path.join(path, excelfilename)

brand2.to_csv(output_file, index=False)


brand1file = brand1.groupby(['sku','cost'])['qty'].sum().reset_index()
brand1file['total'] = brand1file['cost'] * brand1file['qty']

date_str = datetime.now().strftime('%m-%d-%Y')

path = '../cloudbbeh/gonder'
excelfilenameeh = f'{date_str}-brand1.csv'

output_file = os.path.join(path, excelfilenameeh)

brand1file.to_csv(output_file, index=False)


brand2file = brand2.groupby(['sku','cost'])['qty'].sum().reset_index()
brand2file['total'] = brand2file['cost'] * brand2file['qty']

date_str = datetime.now().strftime('%m-%d-%Y')

excelfilenamebb = f'{date_str}-brand2s.csv' 

path = '../cloudbbeh/gonder'
output_file = os.path.join(path, excelfilenamebb)

brand2file.to_csv(output_file, index=False)


from datetime import datetime

wolesale_sku_map = pd.read_csv('./skus/wholesale_sold_map.csv')

final_result['sku'] = final_result['sku'].astype(str)
wolesale_sku_map['sku'] = wolesale_sku_map['sku'].astype(str)

final_result['sku'] = final_result['sku'].str.strip()
wolesale_sku_map['sku'] = wolesale_sku_map['sku'].str.strip()

wholesale_sales = pd.merge(final_result, wolesale_sku_map, left_on='sku', right_on='sku', how='left')

current_date = datetime.now()
wholesale_sales['date'] = current_date.date()
wholesale_sales['Year'] = current_date.year
wholesale_sales['Month'] = calendar.month_name[current_date.month]

wholesale_sales['date'] = pd.to_datetime(wholesale_sales['date'], format='%Y-%m-%d')
wholesale_sales['date'] = wholesale_sales['date'].dt.strftime('%m-%d-%Y')


from datetime import datetime

brand2 = wholesale_sales.loc[sales['brand'].isin(['brand2', 'brand3'])]
brand1 = wholesale_sales.loc[wholesale_sales['brand'] == 'Brand1']

date_string = datetime.now().strftime('%m-%d-%Y')

brand2.to_csv(f'../cloudbbeh/bb/2023/data/wholesale/{date_string}.csv', index=False)
brand1.to_csv(f'../cloudbbeh/eh/2023/data/wholesale/{date_string}.csv', index=False)

