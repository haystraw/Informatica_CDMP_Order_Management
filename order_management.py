import cdmp_api
import csv
import os
import json
import time
import base64
import getpass
import sys
import snowflake_orders
import sqlserver_orders
import oracle_orders
import aws_lakeformation_orders
import databricks_orders
import mapping_task_orders
import datetime
import my_encrypt
import configparser
import requests
import socket

version = 20250425


config = configparser.ConfigParser()
config.read('config.ini')
config_section = 'IDMC'

INFA_username = config[config_section].get('username')
INFA_encrypted_password = config[config_section].get('encrypted_password')
INFA_cai_api_url = config[config_section].get('cai_api_url_base')

poll_frequence_in_seconds = 10
hours_to_run = 8
total_number_of_polls = 0
## total_number_of_polls = ((hours_to_run * 60 * 60) / poll_frequence_in_seconds)
## Set total_number_of_polls to zero, to never stop


debugFlag = False
if config[config_section].get('debug').upper().startswith('T'):
    debugFlag = True



when_debug_stop_after_1_loop = False

'''
In order to encrypt a password. Run python my_encrypt.py
'''

def logPeriodically(annotation="",timePeriod=86400):
    filename = "infalog.txt"
    current_time = time.time()

    if os.path.exists(filename):
        last_modified_time = os.path.getmtime(filename)
        if current_time - last_modified_time < timePeriod:  # Less than a day
            # Continue with the execution
            pass
        else:
            # Overwrite the file and then run your code
            with open(filename, 'w') as f:
                # Write your data to file (if needed)
                f.write("Simple Infalog\n")
            
            # Execute your code here
            infaLog(annotation=annotation)
    else:
        # Overwrite the file and then run your code
        with open(filename, 'w') as f:
            # Write your data to file (if needed)
            f.write("Simple Infalog\n")
        
        # Execute your code here
        infaLog(annotation=annotation)

def infaLog(annotation=""):
    try:
        ## This is simply a "phone home" call.
        ## Just to note which Informatica Org is using this script
        ## If it's unable to reach this URL, it will ignore.
        this_headers = {"Content-Type": "application/json", "X-Auth-Key": "b74a58ca9f170e49f65b7c56df0f452b0861c8c870864599b2fbc656ff758f5d"}
        logs=[{"timestamp": time.time(), "function": f"[{os.path.basename(__file__)}][main]", "execution_time": "N/A", "annotation": annotation, "machine": socket.gethostname()}]
        response=requests.post("https://infa-lic-worker.tim-qin-yujue.workers.dev", data=json.dumps({"logs": logs}), headers=this_headers)
    except:
        pass


def debug(message):
    if debugFlag:
        print(f"DEBUG: {message}")

def createToken():
    username = input("Username:")
    password = getpass.getpass('Password:')
    this_token = base64.b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")

    print(f"Token: {this_token}")

def get_argument(argname, arguments):
	return_value = ''
	for arg in arguments:
		try:
			if "=" in arg.lower() and arg.lower().startswith(argname):
				return arg.split('=')[1]				
			if not "=" in arg.lower() and arg.lower().startswith(argname):
				return argname
		except Exception:
			pass
	return return_value

def createCSVFile(filename):
    if not os.path.exists(filename):
        debug(f"order_management.createCSVFile: \"{filename}\" doesn't exist. Creating file {filename}")
        # If the file doesn't exist, create a new CSV file and write the header
        with open(filename, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['OrderId', 'OrderDate', 'OrderJustification', 'CollectionNames', 'CollectionOwnerNames', 'CollectionOwnerEmails', 'RequestorName', 'RequestorUsername', 'RequestorEmail', "TermsOfUse", "Usage", "DeliveryTargetID", "DeliveryTargetName", "DeliveryTargetLocation", "DeliveryTargetMethod", "DeliveryTargetFormat", "CostCenter", "LastComment", "OrderStatus", "FulfillmentStatus", "FulfillmentComment", "AccessId", "AccessStatus", "WithdrawStatus", "WithdrawComment"])

def insertNewRow(filename, new_row):
    try:
        with open(filename, 'a', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(new_row)    
    except PermissionError:
        time.sleep(3)
        print(f"ERROR:     Permission Error adding Order {new_row[0]}. Is the file open? will try again...")
        insertNewRow(filename, new_row)

def insertOrderInCSV(order_obj, filename):
    createCSVFile(filename)
    orderAlreadyExists = False

    with open(filename, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        rows = list(csv_reader)        
        ## csv_reader = csv.reader(csv_file)
        for row in rows:
            needToUpdateAccessStatus = False            
            
            if order_obj.Id == row['OrderId']:
                orderAlreadyExists = True

            if (order_obj.Id == row['OrderId']) and (len(order_obj.accessId) > 1) and (not row['AccessId'] == order_obj.accessId):
                ## print(f"DEBUG: {row['OrderId']} file:{row['AccessId']} obj:{order_obj.accessId}")
                debug(f"order_management.insertOrderInCSV: Order Already Exists. Updating AccessId. File: \"{filename}\" Order: \"{order_obj.Id}\" AccessId: \"{order_obj.accessId}\"")
                updateField(filename, order_obj.Id, 'AccessId', order_obj.accessId)

            if (order_obj.Id == row['OrderId']) and (len(order_obj.accessId) > 1) and (len(row['AccessStatus']) < 2):
                debug(f"order_management.insertOrderInCSV: Order Already Exists. Updating AccessStatus. File: \"{filename}\" Order: \"{order_obj.Id}\" AccessId: \"{order_obj.accessId}\" AccessStatus: \"PENDING_WITHDRAW\"")
                updateField(filename, order_obj.Id, 'AccessStatus', "PENDING_WITHDRAW")

    if not orderAlreadyExists:
        accessStatus = ""
        orderStatus = ""
        fulFillmentStatus = ""  

        if len(order_obj.accessId) > 1:
            debug(f"order_management.insertOrderInCSV: Order DOESN'T Exist AND There's an AccessID (which means that it was already fulfilled), so inserting and setting Order: \"{order_obj.Id}\" AccessId: \"{order_obj.accessId}\" orderStatus: \"SUCCESS\" fulFillmentStatus: \"FULFILLED\" AccessStatus: \"PENDING_WITHDRAW\"")
            orderStatus = "SUCCESS"
            fulFillmentStatus = "FULFILLED"
            accessStatus = "PENDING_WITHDRAW"        
        new_row = []
        new_row.append(order_obj.Id)
        new_row.append(order_obj.createdOn)
        new_row.append(order_obj.justification)
        new_row.append(order_obj.collection_names)
        new_row.append(order_obj.collection_owner_names)
        new_row.append(order_obj.collection_emails)
        new_row.append(order_obj.requestor['displayName'])
        new_row.append(order_obj.requestor['name'])
        new_row.append(order_obj.requestor['email'])
        new_row.append(order_obj.terms_string)
        new_row.append(order_obj.usage_name)
        new_row.append(order_obj.deliveryTarget['id'])
        new_row.append(order_obj.deliveryTarget['name'])
        new_row.append(order_obj.deliveryTarget['location'])
        new_row.append(order_obj.deliveryTarget['deliveryMethod'])
        new_row.append(order_obj.deliveryTarget['deliveryFormat'])
        new_row.append(order_obj.costCenter)
        new_row.append(order_obj.lastComment)
        new_row.append(orderStatus)
        new_row.append(fulFillmentStatus)
        new_row.append("")
        new_row.append(order_obj.accessId)
        new_row.append(accessStatus)
        new_row.append("")
        new_row.append("")

        print(f"INFO: Ready to insert Order: {order_obj.Id}")
        insertNewRow(filename, new_row)

def updateField(filename, OrderId, fieldName, updateText):
    print(f"INFO: Updating {fieldName} for Order {OrderId}")
    try:
        with open(filename, 'r', newline='') as file:
            reader = csv.DictReader(file)
            rows = list(reader)

            for row in rows:
                if row['OrderId'] == OrderId:
                    row[fieldName] = updateText

            with open(filename, 'w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
                writer.writeheader()
                writer.writerows(rows)                                
    except PermissionError:
        time.sleep(3)
        print(f"ERROR:     Permission Error updating {OrderId} Is the file open? will try again...")
        updateField(filename, OrderId, fieldName, updateText)

def getNewOrders(filename, session):
    debug(f"order_management.getNewOrders: Calling INFA session to fetch approved orders")
    session.fetchApprovedOrders()
    debug(f"order_management.getNewOrders: Fetched Approved Orders Total Orders: {len(session.orders)}")

    debug(f"order_management.getNewOrders: Calling INFA session to fetch Pending Withdraw orders")
    session.fetchPendingAccess()
    debug(f"order_management.getNewOrders: Pending Withdraw orders Total Orders: {len(session.orders)}")
    for order in session.orders:
        insertOrderInCSV(order, filename)

def fulfillOrdersInMarketplace(filename, session):
    createCSVFile(filename)
    orders_to_update = []
    with open(filename, 'r' ) as theFile:
        reader = csv.DictReader(theFile)
        for line in reader:        
            if ((line['FulfillmentStatus'].upper() == 'FULFILLED' or 
                line['FulfillmentStatus'].upper() == 'DELIVERED') and 
                line['OrderStatus'] == ""):
                this_order = {"id": line['OrderId'], "comment": line['FulfillmentComment']}
                orders_to_update.append(this_order)

    for order in orders_to_update:
        print(f"INFO: Ready to update Order: {order['id']}")
        status, statusText, url = session.fulfillOrder(order['id'], order['comment'])
        if status == "SUCCESS":
            updateField(filename, order['id'], 'OrderStatus', status)
        else:
            print(f"ERROR:      Order: {order['id']}, will write an ERROR log file")
            updateField(filename, order['id'], 'OrderStatus', status)
            timestamp = int(time.time())
            error_file = f"ERROR_{order['id']}_{timestamp}.json"
            with open(error_file, 'w') as file:
                json.dump(statusText, file) 
            try:
                file1 = open(error_file, "a")  # append mode
                file1.write(f"\n API URL:{url}")
                file1.close()
            except Exception:
                pass            



def withdrawAccessInMarketplace(filename, session):
    createCSVFile(filename)
    access_to_update = []
    with open(filename, 'r' ) as theFile:
        reader = csv.DictReader(theFile)
        for line in reader:        
            if ((line['WithdrawStatus'].upper() == 'WITHDRAWN')  and 
                line['AccessStatus'] == "PENDING_WITHDRAW"):
                this_access = {"order_id": line['OrderId'], "id": line['AccessId'], "comment": line['WithdrawComment']}
                access_to_update.append(this_access)

    for order_access in access_to_update:
        print(f"INFO: Ready to update Order: {order_access['order_id']} access {order_access['id']}")
        status, statusText = session.withdrawAccess(order_access['id'], order_access['comment'])
        if status == "SUCCESS_WITHDRAWN":
            updateField(filename, order_access['order_id'], 'AccessStatus', status)
        else:
            print(f"ERROR:      Order: {order_access['order_id']}, will write an ERROR log file")
            updateField(filename, order_access['order_id'], 'AccessStatus', status)
            timestamp = int(time.time())
            error_file = f"ERROR_{order_access['order_id']}_{timestamp}.json"
            with open(error_file, 'w') as file:
                json.dump(statusText, file)                          

if len(get_argument('token', sys.argv)) > 1:
   createToken()
   exit 
total_time = ""

if debugFlag and when_debug_stop_after_1_loop:
    total_number_of_polls = 1

if (total_number_of_polls == 0):
    total_time = f"until canceled"
else:
    total_time = f"{str(datetime.timedelta(seconds=total_number_of_polls*poll_frequence_in_seconds))}"
print(f"INFO: Polling started with user {INFA_username}. Will run {total_time} every {poll_frequence_in_seconds} seconds")
logPeriodically(annotation=f"URL: {INFA_cai_api_url}, User: {INFA_username}, Version: {version}")
poll_count = 0
while (total_number_of_polls == 0 or poll_count < total_number_of_polls):
    createCSVFile('orders.csv')
    session = cdmp_api.Infa_CDMP_Session(username=INFA_username, encrypted_password=INFA_encrypted_password, url_base=INFA_cai_api_url, debugFlag=debugFlag)
    getNewOrders("orders.csv", session)
    if config['Snowflake'].getboolean('enable'):
        snowflake_session = snowflake_orders.Snowflake_Session(orders_file='orders.csv', collections_config_file='snowflake_collections.csv', debugFlag=debugFlag)
    if config['Sqlserver'].getboolean('enable'):
        sqlserver_session = sqlserver_orders.Sqlserver_Session(orders_file='orders.csv', collections_config_file='sqlserver_collections.csv', debugFlag=debugFlag)
    if config['Oracle'].getboolean('enable'):
        oracle_session = oracle_orders.Oracle_Session(orders_file='orders.csv', collections_config_file='oracle_collections.csv', debugFlag=debugFlag)
    if config['AWS'].getboolean('enable'):
        aws_session = aws_lakeformation_orders.AWS_Session(orders_file='orders.csv', collections_config_file='aws_lakeformation_collections.csv', debugFlag=debugFlag)
    if config['Databricks'].getboolean('enable'):
        databricks_session = databricks_orders.Databricks_Session(orders_file='orders.csv', collections_config_file='databricks_collections.csv', debugFlag=debugFlag)
    if config['IDMC_CDI'].getboolean('enable'):
        mapping_task_session = mapping_task_orders.MappingTask_Session(orders_file='orders.csv', collections_config_file='mapping_task_collections.csv', debugFlag=debugFlag)



    fulfillOrdersInMarketplace("orders.csv", session)
    withdrawAccessInMarketplace("orders.csv", session)
    poll_count += 1
    time.sleep(poll_frequence_in_seconds)


