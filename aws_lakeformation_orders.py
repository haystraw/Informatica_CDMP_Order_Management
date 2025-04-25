import jaydebeapi
import my_encrypt
import warnings
import pandas
import csv
import time
import os
import sys
import boto3
import botocore
import json
import configparser
warnings.filterwarnings("ignore")

version = 20240807

config = configparser.ConfigParser()
config.read('config.ini')
config_section = 'AWS'

default_region = config[config_section].get('default_region')

debugFlag = False

default_collections_file = "aws_lakeformation_collections.csv"

use_user_map = True
default_user_map_file = "user_map.csv"
default_map_user_type = 'AWS'

'''
Don't forget credentials file:
Windows: C:/Users/xxx/.aws/credentials
Linux: ~/.aws/credentials
[default]
aws_access_key_id = xxx
aws_secret_access_key = xxx
'''


######################
current_filename = os.path.basename(__file__)

class AWS_Session:
    def debug(self, Message):
        if self.debug_enabled:
            print(f"DEBUG: {Message}")

    def lookup_users(self,usernames):
        final_e = None
        for username in usernames:
            try:
                response = self.lookup_user(username)
                return response
            except Exception as e:
                final_e = e
                pass
        raise(final_e)
    

    def lookup_user(self,username):
        client = boto3.client(
            "iam",
            config=botocore.config.Config(connect_timeout=5, read_timeout=60, retries={"max_attempts": 20},region_name=default_region),
        )   
        response = client.get_user(UserName=username) 
        return response            

    def lakeformation_permissions (self,action, principal_json, resource_json, permissions_array, default_region=default_region):
        client = boto3.client(
            "lakeformation",
            config=botocore.config.Config(connect_timeout=5, read_timeout=60, retries={"max_attempts": 20},region_name=default_region),
        )   

        if action[0].lower() == 'g':
            try: 
                response = client.grant_permissions(Principal=principal_json, Resource=resource_json, Permissions=permissions_array )
                return response
            except Exception as e:
                raise(e)
        elif action[0].lower() == 'r':
            try:
                response = client.revoke_permissions(Principal=principal_json, Resource=resource_json, Permissions=permissions_array )
            except Exception as e:
                return_message = "unknown error"
                message = str(e)
                if "Grantee has no permissions and no grantable permissions on resource" in message:
                    return_message = f"{e}"
                    return return_message
                else:
                    raise(e)

            return response
    
    def lakeformation_database_access(self,action, usernames, database ):
        response = self.lookup_users(usernames)
        arn = response['User']['Arn']

        permissions = ["DESCRIBE", "SELECT"]
        principal_json = {"DataLakePrincipalIdentifier": arn}
        
        resource_json = None
        if database.startswith('{'):
            resource_json = json.loads(database)
        else:
            resource_json = { "Table": { "DatabaseName": database, "TableWildcard": {} } }
        
        response = self.lakeformation_permissions(action=action, principal_json=principal_json,resource_json=resource_json,permissions_array=permissions, default_region=default_region)
        return response



    def __init__(self, user_map_file=default_user_map_file,orders_file="", user_groups_config_file="user_groups.csv", collections_config_file=default_collections_file, debugFlag=debugFlag):
        self.debug_enabled = debugFlag
        self.collections_config_file = collections_config_file
        self.collections_config = []
        self.orders_file = orders_file
        self.read_collections_config()
        self.user_groups_config_file = user_groups_config_file
        self.user_groups = []
        self.read_user_groups_config_file()
        self.user_map_type = default_map_user_type
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
				
    def lookupUserMap(self, idmc_user):
        for user_obj in self.user_map:
            if user_obj['IDMC User'].upper() == idmc_user.upper() and user_obj['user_map_type'] == self.user_map_type:
                return user_obj['Source User']
        return ""
    
    def read_user_groups_config_file(self):
        try:
            with open(self.user_groups_config_file, 'r') as f:
                dict_reader = csv.DictReader(f)
                list_of_dict = list(dict_reader)
                self.user_groups = list_of_dict            
        except:
            ## If there's no file, then fine. We don't care.
            pass

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
                return True, coll['database'], coll['comment'], coll['overrideUser']
        return False, "", "", ""

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
                return True, coll['database'], coll['withdraw_comment'], coll['overrideUser']
        return False, "", "", ""
       

    def fulfillOrders(self):
        filename = self.orders_file
        theseCollections = self.collections_config
        orders_to_update = []
        with open(filename, 'r', newline='') as file:
            reader = csv.DictReader(file)
            rows = list(reader)

            for row in rows:
                self.debug(f"{current_filename}.fulfillOrders Evaluating for fulfillment {row}")
                isMatched, database, comment, overrideUser = self.checkCollectionsForFulfillment(row)
                self.debug(f"     Is this row Matched? {isMatched}")
                if isMatched:
                    print(f"INFO: Fulfilling \"{row['CollectionNames']}\" on Order \"{row['OrderId']}\" for User \"{row['RequestorUsername']}\"")
                    try:
                        if len(str(overrideUser)) < 1 and use_user_map:
                            overrideUser = self.lookupUserMap(row['RequestorUsername']) 

                        possible_usernames = [overrideUser]

                        if len(str(overrideUser)) < 1:
                            possible_usernames = []
                            possible_usernames.append(row['RequestorUsername'])
                            possible_usernames.append(row['RequestorEmail'])
                            possible_usernames.append(row['RequestorUsername'].split("@")[0])
                            possible_usernames.append(row['RequestorEmail'].split("@")[0])

                        self.lakeformation_database_access('grant',possible_usernames,database)
                        update_data = {"id": row['OrderId'], "status": "FULFILLED", "comment": comment}
                        orders_to_update.append(update_data)
                        self.updateFulfillmentStatus(row['OrderId'], "FULFILLED", comment)
                    except Exception as e:
                        print(f"     ERROR: {str(e)}")
                        update_data = {"id": row['OrderId'], "status": "ERROR: "+str(e), "comment": ""}
                        orders_to_update.append(update_data)
                        self.updateFulfillmentStatus(row['OrderId'], "ERROR: "+str(e), "")

    def withdrawOrders(self):
        filename = self.orders_file
        theseCollections = self.collections_config
        orders_to_update = []
        with open(filename, 'r', newline='') as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            
        self.debug(f"{current_filename}.withdrawOrders About to loop through orders {len(rows)}")
        for row in rows:
            self.debug(f"{current_filename}.withdrawOrders Evaluating for withdraw {row}")
            isMatched, database, comment, overrideUser = self.checkCollectionsForWithdraw(row)
            self.debug(f"     Is this row Matched? {isMatched}")
            if isMatched:
                print(f"INFO: Withdrawing \"{row['CollectionNames']}\" on Order \"{row['OrderId']}\" for User \"{row['RequestorUsername']}\"")
                try:

                    if len(str(overrideUser)) < 1 and use_user_map:
                        overrideUser = self.lookupUserMap(row['RequestorUsername']) 

                    possible_usernames = [overrideUser]

                    if len(str(overrideUser)) < 1:
                        possible_usernames = []
                        possible_usernames.append(row['RequestorUsername'])
                        possible_usernames.append(row['RequestorEmail'])
                        possible_usernames.append(row['RequestorUsername'].split("@")[0])
                        possible_usernames.append(row['RequestorEmail'].split("@")[0])

                    self.lakeformation_database_access('revoke',possible_usernames,database)

                    update_data = {"id": row['OrderId'], "status": "FULFILLED", "comment": comment}
                    orders_to_update.append(update_data)
                    self.updateWithdrawStatus(row['OrderId'], "WITHDRAWN", comment)
                except Exception as e:
                    print(f"     ERROR: {str(e)}")
                    update_data = {"id": row['OrderId'], "status": "ERROR: "+str(e), "comment": ""}
                    orders_to_update.append(update_data)
                    self.updateWithdrawStatus(row['OrderId'], "ERROR: "+str(e), "")


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
