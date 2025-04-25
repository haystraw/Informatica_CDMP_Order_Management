import jaydebeapi
import my_encrypt
import warnings
import pandas
import csv
import time
import os
import idmc_api
import datetime
import json
import re
import configparser
warnings.filterwarnings("ignore")

version = 20240722


config = configparser.ConfigParser()
config.read('config.ini')
config_section = 'IDMC_CDI'


INFA_username = config['IDMC'].get('username')
INFA_password = ''
INFA_encrypted_password = config['IDMC'].get('encrypted_password') 
default_infa_url_base = config[config_section].get('url_base')
default_pod_url_base = config[config_section].get('pod_url_base')
default_infa_hawk_url_base = config[config_section].get('hawk_url_base')

this_time_stamp_format = '%Y%m%d%H%M%S'

default_fetch_DG_Objects_On_Connect = False

use_user_map = True
default_user_map_file = "user_map.csv"

jdbc_connections = []


for section in config.sections():
    try:
        this_dict = {}
        this_dict['jdbc_name'] = section
        this_dict['jdbc_driver'] = config[section].get('jdbc_driver')
        this_dict['jdbc_url'] = config[section].get('jdbc_url')
        this_dict['jdbc_username'] = config[section].get('username')
        this_dict['jdbc_password'] = config[section].get('password')
        this_dict['encrypted_jdbc_password'] = config[section].get('encrypted_password')
        this_dict['jdbc_driver_file'] = config[section].get('jdbc_driver_file')
        if config.has_option(section, 'jdbc_type'):
            this_dict['user_map_type'] = config[section].get('jdbc_type')
        else:
            this_dict['user_map_type'] = section
        jdbc_connections.append(this_dict)
    except Exception:
        pass

'''
jdbc_connections = [
    {
        "jdbc_name": "Oracle_COPA",
        "jdbc_driver": "oracle.jdbc.OracleDriver",
        "jdbc_url": "jdbc:oracle:thin:@advanced3.mxdomain:1521/orcl",
        "jdbc_username": "COPA",
        "jdbc_password": "",
        "encrypted_jdbc_password": 'gAAAAABmEC6jsbXjSiNSQoxw1zw5Z1RmlowPoRt1SU8XGYLxDPoZ4xH4ilY9_81iS2QJYZsuEalNjMnywmJ4wBg7zDzJEgprwjw1IhcuoIlRYQajDq-EanhXeWEQKSZ97rZpkVjMYvO9',
        "jdbc_driver_file": './ojdbc11.jar',
        "user_map_type": "Oracle"
    },{
        "jdbc_name": "Databricks",
        "jdbc_driver": "com.databricks.client.jdbc.Driver",
        "jdbc_url": "jdbc:databricks://adb-3507816793016728.8.azuredatabricks.net:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/endpoints/556b3ac6518656a2;",
        "jdbc_username": "token",
        "jdbc_password": "",
        "encrypted_jdbc_password": 'gAAAAABl4M4vykUh1w2K6vXGLV3cs0jDEz2oz9b9gdFJeFhlSDqmrtxjbftOzGlp4ZGClfcey4aKEfm88oMLXpiYuTMJlMsKZM87pwtC3clQQoGcl1zE_zXFReLJlQjvvAtaUYGB4FMF',
        "jdbc_driver_file": './DatabricksJDBC42.jar',
        "user_map_type": "Databricks"
    },{
        "jdbc_name": "Snowflake",
        "jdbc_driver": "net.snowflake.client.jdbc.SnowflakeDriver",
        "jdbc_url": "jdbc:snowflake://eeb76041.snowflakecomputing.com:443/?db=NSEN",
        "jdbc_username": "SHAYES",
        "jdbc_password": "",
        "encrypted_jdbc_password": 'gAAAAABkkaVgWaRLU1Hoz71IPtYoQpesFHQ7aVtWWx5CG0NaIEi0msa9d2oLNFbCAXp3Obfed4e-SL-0VqaDTpbGPZGKmkZr3w==',
        "jdbc_driver_file": '.\snowflake-jdbc-3.9.2.jar',
        "user_map_type": "Snowflake"
    },{
        "jdbc_name": "SQLServer",
        "jdbc_driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "jdbc_url": "jdbc:sqlserver://az-sqldb-server.database.windows.net:1433;databaseName=NSEN;encrypt=true;",
        "jdbc_username": "sqladminuser",
        "jdbc_password": "",
        "encrypted_jdbc_password": 'gAAAAABkkaVgWaRLU1Hoz71IPtYoQpesFHQ7aVtWWx5CG0NaIEi0msa9d2oLNFbCAXp3Obfed4e-SL-0VqaDTpbGPZGKmkZr3w==',
        "jdbc_driver_file": './mssql-jdbc-12.2.0.jre8.jar',
        "user_map_type": "Sqlserver"
    }
]
'''


debugFlag = False

query_user_based_on_email = "SELECT name from sysusers where name = LEFT('%(email)s', CHARINDEX('@', '%(email)s') - 1)"
grant_role_to_user = ["GRANT %(role)s to %(requesting_user)s", "call build_nsen_synonymns('%(requesting_user)s')"]
revoke_role_from_user = ["REVOKE %(role)s from %(requesting_user)s","call build_nsen_synonymns('%(requesting_user)s')"]
default_collections_file = "mapping_task_collections.csv"

######################


if len(INFA_encrypted_password) > 2:
    try:
        INFA_password = my_encrypt.decrypt_message(INFA_encrypted_password) 
    except Exception:
        pass

current_filename = os.path.basename(__file__)

class MappingTask_Session:
    def debug(self, Message):
        if self.debug_enabled:
            print(f"DEBUG: {Message}")

    def __init__(self, orders_file="orders.csv", user_map_file=default_user_map_file,user_groups_config_file="user_groups.csv", collections_config_file=default_collections_file, debugFlag=debugFlag):
        self.debug_enabled = debugFlag
        self.collections_config_file = collections_config_file
        self.collections_config = []
        self.orders_file = orders_file
        self.read_collections_config()
        self.user_groups_config_file = user_groups_config_file
        self.user_groups = []
        self.read_user_groups_config_file()
        self.user_map_file = user_map_file
        self.user_map = []
        self.read_user_map_file()

        
        self.fulfillOrders()
        self.withdrawOrders()



 
    def read_collections_config(self):

        with open(self.collections_config_file, 'r') as f:
            dict_reader = csv.DictReader(f)
            list_of_dict = list(dict_reader)
            self.collections_config = list_of_dict

    def read_user_groups_config_file(self):
        try:
            with open(self.user_groups_config_file, 'r') as f:
                dict_reader = csv.DictReader(f)
                list_of_dict = list(dict_reader)
                for row in list_of_dict:
                    self.user_groups.append(row)
        except:
            ## If there's no file, then fine. We don't care.
            pass

    def read_user_map_file(self):
        if use_user_map:
            try:
                with open(self.user_map_file, 'r') as f:
                    dict_reader = csv.DictReader(f)
                    list_of_dict = list(dict_reader)
                    for row in list_of_dict:
                        self.user_map.append(row)                            
            except:
                ## If there's no file, then fine. We don't care.
                pass

    def executeStatement(self, jdbc_driver="", jdbc_url="", username="", password="", jdbc_driver_file="", statements=[]):
        self.debug(f"{current_filename}.executeStatement About to execute statements: {statements}")
        with jaydebeapi.connect(jdbc_driver, jdbc_url, (username, password), jdbc_driver_file) as conn:
            with conn.cursor() as cursor:
                for statement in statements:
                    if len(statement) > 2:
                        cursor.execute(statement)
                        ## result = cursor.fetchall()
                        ## return result

        ## result = execute_statement(statement)

    def isMatchedUserAgainstUsergroup(self, requestor_user_or_email_array, coll_group_name):
        if coll_group_name == 'ANY':
            return True
        else:
            for group_assignment in self.user_groups:
                if group_assignment['group'] == coll_group_name:
                    self.debug(f"Testing to see users of group {coll_group_name}")
                    for key in group_assignment:
                        value = group_assignment[key]
                        self.debug(f"Testing to see if group assignment {value} is in array {requestor_user_or_email_array} {value in requestor_user_or_email_array}")
                        if value in requestor_user_or_email_array:
                            return True
        return False


    def checkCollectionsForFulfillment(self, row):
        theseCollections = self.collections_config
        self.debug(f"     {current_filename}.checkCollectionsForFulfillment Checking Collections file. Total Collections: {len(theseCollections)}")
        terms_of_use = []
        try:
            terms_of_use = row['TermsOfUse'].split(';')
        except:
            pass         

        for coll in theseCollections:
            ## If it's already been fulfilled, reject it.
            if row['FulfillmentStatus'] == 'FULFILLED':
                self.debug(f"     Rejected: FulfillmentStatus is {row['FulfillmentStatus']}")
                return False, None
            
            self.debug(f"     Trying to Match against Collection: \"{coll['collection']}\" \"{coll['DeliveryTargetName']}\" \"{coll['TermsOfUse']}\" \"{coll['Usage']}\" \"{coll['Usergroup']}\"")
            self.debug(f"        Match collection name?: {row['CollectionNames'] == coll['collection']} \"{coll['collection']}\" == \"{row['CollectionNames']}\"")
            self.debug(f"        Match DeliveryTargetName name?: {row['DeliveryTargetName'] == coll['DeliveryTargetName']} {coll['DeliveryTargetName'] == 'ANY'} \"{coll['DeliveryTargetName']}\" == \"{row['DeliveryTargetName']}\"")
            self.debug(f"        Match TermsOfUse name?: {coll['TermsOfUse'] in terms_of_use} {coll['TermsOfUse'] == 'ANY'} \"{coll['TermsOfUse']}\" in \"{terms_of_use}\"")
            self.debug(f"        Match Usage name?: {row['Usage'] == coll['Usage']} {coll['Usage'] == 'ANY'} \"{coll['Usage']}\" == \"{row['Usage']}\"")
            self.debug(f"        Match Usergroup name?: {self.isMatchedUserAgainstUsergroup([row['RequestorName'],row['RequestorUsername'],row['RequestorEmail']], coll['Usergroup'])} self.isMatchedUserAgainstUsergroup([{row['RequestorName']},{row['RequestorUsername']},{row['RequestorEmail']}], {coll['Usergroup']})" )
            self.debug(f"             Usergroups are: {self.user_groups}")


            if ( row['CollectionNames'] == coll['collection'] and 
                ( row['DeliveryTargetName'] == coll['DeliveryTargetName'] or coll['DeliveryTargetName'] == 'ANY') and
                ( coll['TermsOfUse'] in terms_of_use or coll['TermsOfUse'] == 'ANY') and
                ( row['Usage'] == coll['Usage'] or coll['Usage'] == 'ANY') and
                (self.isMatchedUserAgainstUsergroup([row['RequestorName'],row['RequestorUsername'],row['RequestorEmail']], coll['Usergroup']))
                ):
                return True, coll
        return False, None

    def checkCollectionsForWithdraw(self, row):
        theseCollections = self.collections_config
        self.debug(f"     {current_filename}.checkCollectionsForWithdraw Evaluating Order against Collections in file. Total Collections: {len(theseCollections)}")
        terms_of_use = []
        try:
            terms_of_use = row['TermsOfUse'].split(';')
        except:
            pass

        for coll in theseCollections:
            ## If it's already been fulfilled, reject it.
            if row['WithdrawStatus'] == 'WITHDRAWN':
                self.debug(f"     Rejected Order: Order's FulfillmentStatus is {row['FulfillmentStatus']}")
                return False, "", "", ""
            
            if not len(row['AccessId']) > 1:
                self.debug(f"     Rejected Order: Order's AccessId Length !> 1 ({len(row['AccessId'])})")
                return False, "", "", ""
            
            self.debug(f"     Trying to Match against Collection: \"{coll['collection']}\" \"{coll['DeliveryTargetName']}\" \"{coll['TermsOfUse']}\" \"{coll['Usage']}\" \"{coll['Usergroup']}\"")
            self.debug(f"        Match collection name?: {row['CollectionNames'] == coll['collection']} \"{coll['collection']}\" == \"{row['CollectionNames']}\"")
            self.debug(f"        Match DeliveryTargetName name?: {row['DeliveryTargetName'] == coll['DeliveryTargetName']} {coll['DeliveryTargetName'] == 'ANY'} \"{coll['DeliveryTargetName']}\" == \"{row['DeliveryTargetName']}\"")
            self.debug(f"        Match TermsOfUse name?: {coll['TermsOfUse'] in terms_of_use} {coll['TermsOfUse'] == 'ANY'} \"{coll['TermsOfUse']}\" in \"{terms_of_use}\"")
            self.debug(f"        Match Usage name?: {row['Usage'] == coll['Usage']} {coll['Usage'] == 'ANY'} \"{coll['Usage']}\" == \"{row['Usage']}\"")
            self.debug(f"        Match Usergroup name?: {self.isMatchedUserAgainstUsergroup([row['RequestorName'],row['RequestorUsername'],row['RequestorEmail']], coll['Usergroup'])} self.isMatchedUserAgainstUsergroup([{row['RequestorName']},{row['RequestorUsername']},{row['RequestorEmail']}], {coll['Usergroup']})" )
            self.debug(f"             Usergroups are: {self.user_groups}")


            if ( row['CollectionNames'] == coll['collection'] and 
                ( row['DeliveryTargetName'] == coll['DeliveryTargetName'] or coll['DeliveryTargetName'] == 'ANY') and
                ( coll['TermsOfUse'] in terms_of_use or coll['TermsOfUse'] == 'ANY') and
                ( row['Usage'] == coll['Usage'] or coll['Usage'] == 'ANY') and
                (self.isMatchedUserAgainstUsergroup([row['RequestorName'],row['RequestorUsername'],row['RequestorEmail']], coll['Usergroup']))
                ):
                return True, coll['role'], coll['withdraw_comment'], coll['overrideUser']
        return False, "", "", ""

    def getConn(self, connection_name):
        for conn in jdbc_connections:
            if conn['jdbc_name'] == connection_name:
                return conn
        return None

    def fulfillOrders(self):
        filename = self.orders_file
        theseCollections = self.collections_config
        orders_to_update = []
        with open(filename, 'r', newline='') as file:
            reader = csv.DictReader(file)
            rows = list(reader)

            for row in rows:
                self.debug(f"{current_filename}.fulfillOrders Evaluating for fulfillment {row}")
                isMatched, coll = self.checkCollectionsForFulfillment(row)
                self.debug(f"     Is this row Matched? {isMatched}")
                if isMatched and coll != None:
                    print(f"INFO: Fulfilling \"{row['CollectionNames']}\" on Order \"{row['OrderId']}\" for User \"{row['RequestorUsername']}\"")

                    overrideUser = coll['overrideUser']
                    original_mapping_task_id = coll['original_mapping_task_id']
                    container_id_for_new_mappings = coll['container_id_for_new_mappings']
                    target_table_parameter_labels = coll['target_table_parameter_labels'].split(';')
                    field_mapping_parameter_labels = coll['field_mapping_parameter_labels'].split(';')
                    pre_post_jdbc_connection_name = coll['pre_post_jdbc_connection_name']
                    pre_create_statements = coll['pre_create_statements'].split(';')
                    post_statements = coll['post_statements'].split(';')
                    list_of_acceptable_columns = coll['list_of_acceptable_columns']
                    comment = coll['comment']
                    conn = self.getConn(pre_post_jdbc_connection_name)
                    jdbc_driver = conn['jdbc_driver']
                    jdbc_url = conn['jdbc_url']
                    jdbc_username = conn['jdbc_username']
                    jdbc_password = conn['jdbc_password']
                    jdbc_encrypted_password = conn['encrypted_jdbc_password']
                    if len(jdbc_encrypted_password) > 2:
                        jdbc_password = my_encrypt.decrypt_message(jdbc_encrypted_password)                    
                    jdbc_driver_file = conn['jdbc_driver_file']
                    jdbc_user_map_type = conn['user_map_type']
                    new_user_name = self.lookupUserMap(row['RequestorUsername'], jdbc_user_map_type)
                    this_time_stamp = datetime.datetime.now().strftime(this_time_stamp_format)
                    idmc_session = idmc_api.INFASession(username=INFA_username , password=INFA_password, url_base=default_infa_url_base, pod_url_base=default_pod_url_base, hawk_url_base=default_infa_hawk_url_base, fetchDGObjects=default_fetch_DG_Objects_On_Connect, debugFlag=debugFlag)
                    original_mapping_task = idmc_session.CDI_getMappingTask(original_mapping_task_id)
                    new_mapping_task = original_mapping_task.copy()

                    ## New name for Mapping Task, based on the Original
                    new_mt_name = f"{original_mapping_task['name']}_{this_time_stamp}"
                    new_mapping_task['name'] = new_mt_name

                    ## Add a container, to control where the new mapping task will live.
                    new_mapping_task['containerId'] = container_id_for_new_mappings

                    ## Look at List, we might need to look at the last comment.
                    final_list_of_acceptable_columns = list_of_acceptable_columns
                    ## If the Collections parameter doesn't contain commas and it's not ALL, then
                    ## it must be set to look at the approving comment for the list.
                    if ';' not in list_of_acceptable_columns and ',' not in list_of_acceptable_columns and list_of_acceptable_columns.upper() != 'ALL':
                        final_list_of_acceptable_columns = row['LastComment']

                    ## Find the p_field_mappings string parameter

                    ## print(f"DEBUGSCOTT: checking {final_list_of_acceptable_columns.upper()}")

                    if final_list_of_acceptable_columns.upper() != 'ALL':

                        ## print(f"DEBUGSCOTT: Good {new_mapping_task['parameters']}")
                        for param in new_mapping_task['parameters']:
                            ## print(f"DEBUGSCOTT: testing param {param}")
                            parameter_mappings = ""
                            this_param_label = ""
                            try:
                                this_param_label = param['uiProperties']['paramLabel']
                                if len(this_param_label) < 1:
                                    this_param_label = param['uiProperties']['paramName']
                            except:
                                pass
                            ## print(f"DEBUGSCOTT: testing {this_param_label} {field_mapping_parameter_labels}")
                            if this_param_label in field_mapping_parameter_labels:
                                parameter_mappings = param['text']

                                ## Loop through field mappings and prune where not acceptable
                                mappings = parameter_mappings.split(';')
                                acceptable_mappings = []
                                for mapping in mappings:
                                    ## print(f"DEBUGSCOTT: Attemping to split: {mapping}")
                                    this_mapping_col_array = mapping.split('=')
                                    if (len(this_mapping_col_array) > 1):
                                        this_mapping_col = this_mapping_col_array[1]
                                        
                                        this_mapping_regex = '(^|[^a-zA-Z_])'+this_mapping_col.upper()+'([^a-zA-Z_]|$)'
                                        search_results = re.findall(this_mapping_regex, final_list_of_acceptable_columns.upper())
                                        if len(search_results) > 0:
                                            acceptable_mappings.append(mapping)
                                acceptable_mappings_str = ';'.join(acceptable_mappings)+";"
                                print(f"INFO: Updating Mapping for parameter {param['label']}: {acceptable_mappings_str}")
                                param['text'] = acceptable_mappings_str

                    ## Updating the Target table parameter(s)
                    raw_create_table_statements = pre_create_statements
                    create_table_statements = []
                    raw_post_table_statements = post_statements
                    post_table_statements = []
                    new_tables = []
                    for target in target_table_parameter_labels:
                        for param in new_mapping_task['parameters']:
                            try:
                                if param['uiProperties']['objlabel'] == target:
                                    old_name = param['targetObject']
                                    new_name = old_name+"_"+this_time_stamp
                                    param['targetObject'] = new_name
                                    param['targetObjectLabel'] = new_name
                                    param['objectName'] = new_name
                                    param['objectLabel'] = new_name
                                    formatted_old_name_array = old_name.split("/")
                                    formatted_old_name = formatted_old_name_array[-1]
                                    formatted_new_name_array = new_name.split("/")
                                    formatted_new_name = formatted_new_name_array[-1]                                    
                                    for raw_statement in raw_create_table_statements:
                                            create_statement1 = raw_statement.replace("{"+target+"}", formatted_old_name)
                                            create_statement2 = create_statement1.replace("{new_table_name}", formatted_new_name)
                                            create_statement3 = create_statement2.replace("{user}", new_user_name)
                                            create_table_statements.append(create_statement3)
                                            new_tables.append(formatted_new_name)
                                    for raw_statement in raw_post_table_statements:
                                            create_statement1 = raw_statement.replace("{"+target+"}", formatted_old_name)
                                            create_statement2 = create_statement1.replace("{new_table_name}", formatted_new_name)
                                            create_statement3 = create_statement2.replace("{user}", new_user_name)
                                            post_table_statements.append(create_statement3)

                            except:
                                pass

                    new_mapping_task_json = json.dumps(new_mapping_task, indent = 4)
                    print(f"INFO: Creating new Mapping Task {new_mt_name}")
                    new_mapping_task_obj = idmc_session.CDI_createMappingTask(new_mapping_task_json)
                    new_mapping_task_id = new_mapping_task_obj['id']

                    print(f"INFO: Running Pre-statements in {pre_post_jdbc_connection_name}: {create_table_statements}")
                    self.executeStatement(jdbc_driver=jdbc_driver, jdbc_url=jdbc_url, username=jdbc_username, password=jdbc_password, jdbc_driver_file=jdbc_driver_file, statements=create_table_statements)

                    print(f"INFO: Running Mapping task Job: {new_mt_name}")
                    job_run_result = idmc_session.CDI_runMappingTask(new_mapping_task_id)
                    jobState = idmc_session.CDI_waitForMappingTaskJob(new_mapping_task_id)
                    print(f"INFO: Job complete. State: {jobState}")
                    
                    ## Running post statements
                    print(f"INFO: Executing Post-statements in {pre_post_jdbc_connection_name}: {post_table_statements}")
                    self.executeStatement(jdbc_driver=jdbc_driver, jdbc_url=jdbc_url, username=jdbc_username, password=jdbc_password, jdbc_driver_file=jdbc_driver_file, statements=post_table_statements)
                    

                    ## print(f"DEBUG: Comment: {comment4}")
                    comment1 = comment.replace("{"+target+"}", old_name)
                    comment2 = comment1.replace("{new_table_name}", new_name)
                    comment3 = comment2.replace("{user}", new_user_name)                    
                    comment4 = comment3.replace("{tables}", ','.join(new_tables))                    
                    self.updateFulfillmentStatus(row['OrderId'], "FULFILLED", comment4)
                    ## wait = input(f"Pausing ...")



    def lookupUserMap(self, idmc_user, source_type):
        for user_obj in self.user_map:
            if user_obj['IDMC User'].upper() == idmc_user.upper() and user_obj['user_map_type'] == source_type:
                return user_obj['Source User']
        return ""

    def withdrawOrders(self):
        pass



    def updateFulfillmentStatus(self, OrderId, status, comment):
        filename = self.orders_file
        try:
            with open(filename, 'r', newline='') as file:
                reader = csv.DictReader(file)
                rows = list(reader)

                for row in rows:
                    if row['OrderId'] == OrderId:
                        row['FulfillmentStatus'] = status
                        row['FulfillmentComment'] = comment

                with open(filename, 'w', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)                                
        except PermissionError:
            print(f"ERROR:     Permission Error updating {OrderId} Is the file open? will try again...")
            time.sleep(3)
            self.updateFulfillmentStatus(OrderId, status, comment)

    def updateWithdrawStatus(self, OrderId, status, comment):
        filename = self.orders_file
        try:
            with open(filename, 'r', newline='') as file:
                reader = csv.DictReader(file)
                rows = list(reader)

                for row in rows:
                    if row['OrderId'] == OrderId:
                        row['WithdrawStatus'] = status
                        row['WithdrawComment'] = comment

                with open(filename, 'w', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)                                
        except PermissionError:
            print(f"ERROR:     Permission Error updating {OrderId} Is the file open? will try again...")
            time.sleep(3)
            self.updateWithdrawStatus(OrderId, status, comment)            


def executeStatement(jdbc_driver="", jdbc_url="", username="", password="", jdbc_driver_file="", statements=[]):
    with jaydebeapi.connect(jdbc_driver, jdbc_url, (username, password), jdbc_driver_file) as conn:
        with conn.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
                ## result = cursor.fetchall()
                ## return result

    ## result = execute_statement(statement)


def debug():

    target_table_parameter_labels = ["tgt_table"]
    field_mapping_parameter_labels = ["p_field_mappings"]
    container_id_for_new_mappings = "96tlNYKGZPagMafjkrMUqf"
    original_mapping_task_id = "6E1lFlmdwslj3N9jaA5gA0"
    ## list_of_acceptable_columns = 'POC_EMAIL,POC_TITLE'
    list_of_acceptable_columns = 'ALL'
    pre_post_jdbc_connection_name = 'Oracle_COPA'
    new_table_names = "{old_name}_{this_time_stamp}"
    pre_create_statements = "create table {new_table_name} as select * from {tgt_table} where 1=2"
    post_statements = "grant select on {new_table_name} to {user}"



    this_time_stamp = datetime.datetime.now().strftime(this_time_stamp_format)
    session = idmc_api.INFASession(username=INFA_username , password=INFA_password, url_base=default_infa_url_base, pod_url_base=default_pod_url_base, hawk_url_base=default_infa_hawk_url_base, fetchDGObjects=default_fetch_DG_Objects_On_Connect, debugFlag=debugFlag)
    original_mapping_task = session.CDI_getMappingTask(original_mapping_task_id)
    new_mapping_task = original_mapping_task.copy()

    ## New name for Mapping Task, based on the Original
    new_name = f"{original_mapping_task['name']}_{this_time_stamp}"
    new_mapping_task['name'] = new_name

    ## Add a container, to control where the new mapping task will live.
    new_mapping_task['containerId'] = container_id_for_new_mappings

    ## Find the p_field_mappings string parameter
    if list_of_acceptable_columns.upper() != 'ALL':
        for param in new_mapping_task['parameters']:
            parameter_mappings = ""
            if param['label'] in field_mapping_parameter_labels:
                parameter_mappings = param['text']

                ## Loop through field mappings and prune where not acceptable
                cols = list_of_acceptable_columns.split(',')
                mappings = parameter_mappings.split(';')
                acceptable_mappings = []
                for col in cols:
                    for mapping in mappings:
                        ## print(f"Testing: {mapping} | \"=\"{col};")
                        if mapping.endswith("="+col):
                            acceptable_mappings.append(mapping)
                acceptable_mappings_str = ';'.join(acceptable_mappings)+";"
                print(f"INFO: Updating Mapping for parameter {param['label']}: {acceptable_mappings_str}")
                param['text'] = acceptable_mappings_str

    ## Updating the Target table parameter(s)
    raw_create_table_statements = pre_create_statements.split(';')
    create_table_statements = []
    post_statements
    raw_post_table_statements = post_statements.split(';')
    post_table_statements = []
    new_tables = []
    for target in target_table_parameter_labels:
        for param in new_mapping_task['parameters']:
            try:
                if param['uiProperties']['objlabel'] == target:
                    old_name = param['targetObject']
                    new_name = old_name+"_"+this_time_stamp
                    param['targetObject'] = new_name
                    param['targetObjectLabel'] = new_name
                    param['objectName'] = new_name
                    param['objectLabel'] = new_name
                    for raw_statement in raw_create_table_statements:
                        if "{"+target+"}" in  raw_statement:
                            create_statement_part1 = raw_statement.replace("{"+target+"}", old_name)
                            create_statement = create_statement_part1.replace("{new_table_name}", new_name)
                            create_table_statements.append(create_statement)
                            new_tables.append(new_name)
                    for raw_statement in raw_post_table_statements:
                        if "{"+target+"}" in  raw_statement:
                            create_statement_part1 = raw_statement.replace("{"+target+"}", old_name)
                            create_statement = create_statement_part1.replace("{new_table_name}", new_name)
                            post_table_statements.append(create_statement)

            except:
                pass
                
    new_mapping_task_json = json.dumps(new_mapping_task, indent = 4)

    new_mapping_task_obj = session.CDI_createMappingTask(new_mapping_task_json)
    new_mapping_task_id = new_mapping_task_obj['id']

    for conn in jdbc_connections:
        if conn['jdbc_name'] == pre_post_jdbc_connection_name:
            jdbc_driver = conn['jdbc_driver']
            jdbc_url = conn['jdbc_url']
            username = conn['jdbc_username']
            password = conn['jdbc_password']
            encrypted_password = conn['encrypted_jdbc_password']
            jdbc_driver_file = conn['jdbc_driver_file']
            if len(encrypted_password) > 2:
                password = my_encrypt.decrypt_message(encrypted_password)
            executeStatement(jdbc_driver=jdbc_driver, jdbc_url=jdbc_url, username=username, password=password, jdbc_driver_file=jdbc_driver_file, statements=create_table_statements)

    print(f"Should have already executed: {create_table_statements}")
    ## wait = input(f"press any key to run ...")
    job_run_result = session.CDI_runMappingTask(new_mapping_task_id)
    jobState = session.CDI_waitForMappingTaskJob(new_mapping_task_id)
    print(f"Job State: {jobState}")
    for conn in jdbc_connections:
        if conn['jdbc_name'] == pre_post_jdbc_connection_name:
            jdbc_driver = conn['jdbc_driver']
            jdbc_url = conn['jdbc_url']
            username = conn['jdbc_username']
            password = conn['jdbc_password']
            encrypted_password = conn['encrypted_jdbc_password']
            jdbc_driver_file = conn['jdbc_driver_file']
            if len(encrypted_password) > 2:
                password = my_encrypt.decrypt_message(encrypted_password)
            executeStatement(jdbc_driver=jdbc_driver, jdbc_url=jdbc_url, username=username, password=password, jdbc_driver_file=jdbc_driver_file, statements=post_table_statements)

    print(f"INFO: New Tables populated: {new_tables}")




## debug()
