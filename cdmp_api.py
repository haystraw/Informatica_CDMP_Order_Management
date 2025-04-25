import requests
import json
import datetime
import time

debugFlag = False

idmc_api_version = 20240327.0
default_infa_url_base = "https://dm-us.informaticacloud.com"
default_infa_hawk_url_base = "https://cdgc-api.dm-us.informaticacloud.com"
default_pod_url_base = "https://na1.dm-us.informaticacloud.com"

default_fetch_DG_Objects_On_Connect = False

class INFA_DG_Object:

    def debug(self, message):
        if debugFlag:
            print(f"DEBUG: {message}")

    def getvalue(self, key):
        return self.map[key]

    def fetchOtherRelationships(self):

        ## Search for Glossary Relatopnsips
        payload = json.dumps({
            "from": 0,
            "size": 10000,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"elementType": "RELATIONSHIP" }},
                        {"term": {"type":"com.infa.ccgf.models.governance.IClassTechnicalGlossaryBase" }}

                    ]
                }
            },
            "sort": [
                {
                    "com.infa.ccgf.models.governance.scannedTime": {
                        "order": "desc"
                    }
                }
            ]
        })
        response = self.session.DG_elastic_search(payload)
        response_data = response

        for search_obj in response_data['hits']['hits']:

            try:
                raw_map = search_obj['sourceAsMap']
                if search_obj['sourceAsMap']['elementType'] == 'RELATIONSHIP' and 'ACCEPTED' in raw_map['core.curationStatus'] and ( self.origin == raw_map['core.sourceOrigin']  or self.origin == raw_map['core.targetOrigin']):

                    source_id = raw_map['core.sourceIdentity']
                    target_id = raw_map['core.targetIdentity']

                    source_obj = self.session.getObjectByID(source_id)
                    target_obj = self.session.getObjectByID(target_id)


                    if target_obj.classType == 'core.DataElementClassification' or target_obj.classType == 'core.DataEntityClassification':
                        if not target_obj in source_obj.classifications:
                            source_obj.classifications.append(target_obj)
                    elif target_obj.classType == 'com.infa.ccgf.models.governance.BusinessTerm':
                        if not target_obj in source_obj.businessterms:
                            source_obj.businessterms.append(target_obj)
            except:
                pass

        ## Search for Classification Relatopnsips
        payload = json.dumps({
            "from": 0,
            "size": 10000,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"elementType": "RELATIONSHIP" }},
                        {"term": {"type":"core.ClassifiedAs" }}

                    ]
                }
            },
            "sort": [
                {
                    "com.infa.ccgf.models.governance.scannedTime": {
                        "order": "desc"
                    }
                }
            ]
        })
        response = self.session.DG_elastic_search(payload)
        response_data = response

        for search_obj in response_data['hits']['hits']:

            try:
                raw_map = search_obj['sourceAsMap']
                if search_obj['sourceAsMap']['elementType'] == 'RELATIONSHIP' and 'ACCEPTED' in raw_map['core.curationStatus'] and ( self.origin == raw_map['core.sourceOrigin']  or self.origin == raw_map['core.targetOrigin']):
                    source_id = raw_map['core.sourceIdentity']
                    target_id = raw_map['core.targetIdentity']

                    source_obj = self.session.getObjectByID(source_id)
                    target_obj = self.session.getObjectByID(target_id)


                    if target_obj.classType == 'core.DataElementClassification' or target_obj.classType == 'core.DataEntityClassification':
                        if not target_obj in source_obj.classifications:
                            source_obj.classifications.append(target_obj)
                    elif target_obj.classType == 'com.infa.ccgf.models.governance.BusinessTerm':
                        if not target_obj in source_obj.businessterms:
                            source_obj.businessterms.append(target_obj)
            except:
                pass


    def fetchObjects(self):
        print(f"INFO: Fetching Detailed Information for {self.name}")
        payload = json.dumps({
        "from": 0,
        "size": 1000,
        "query": {
            "term": {
            "core.origin": self.origin
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        })

        response = self.session.DG_elastic_search(payload)
        response_data = response

        for obj in response_data['hits']['hits']:
            try:
                if obj['sourceAsMap']['elementType'] == 'OBJECT':
                    raw_map = obj['sourceAsMap']

                    object = INFA_DG_Object(self.session, raw_map)
                    self.objects.append(object)
                    self.session.all_objects.append(object)
            except:
                pass

        for obj in response_data['hits']['hits']:
            try:
                if obj['sourceAsMap']['elementType'] == 'RELATIONSHIP':
                    raw_map = obj['sourceAsMap']
                    source_id = raw_map['core.sourceIdentity']
                    target_id = raw_map['core.targetIdentity']

                    ## I think I already fetched the objects I need...
                    ## source_obj = self.session.getObjectByID(source_id)
                    ## target_obj = self.session.getObjectByID(target_id)

                    target_obj = None
                    source_obj = None
                    for o in self.session.all_objects:
                        if o.identity == source_id:
                            source_obj = o
                        if o.identity == target_id:
                            target_obj = o

                    try:
                        if obj['sourceAsMap']['core.associationKind'] == "core.ParentChild":
                            if source_obj != None and target_obj != None:
                                source_obj.child_objects.append(target_obj)
                                target_obj.parent_objects.append(source_obj)
                    except:
                        pass

                    '''
                    if source_id == '9b7f40c3-c1fe-45e5-9c8c-c945b11c4c2d' or target_id == '9b7f40c3-c1fe-45e5-9c8c-c945b11c4c2d':
                        print(f"DEBUG** {source_obj.name} ({source_obj.externalId}) --> {target_obj.name} ({target_obj.externalId})")
                    '''

                    if 'ACCEPTED' in raw_map['core.curationStatus'] and (target_obj.classType == 'core.DataElementClassification' or target_obj.classType == 'core.DataEntityClassification'):
                        source_obj.classifications.append(target_obj)
                    elif 'ACCEPTED' in raw_map['core.curationStatus'] and (target_obj.classType == 'com.infa.ccgf.models.governance.BusinessTerm'):
                        source_obj.businessterms.append(target_obj)
            except:
                pass

        ## Don't need to run this, as we've already got all of the relationships above.
        self.fetchOtherRelationships()


    def getObjectsByShortType(self, shortType):
        result_array = []
        for i in self.objects:
            if i.shortType.lower() == shortType.lower():
                result_array.append(i)
        return result_array


    def getObjectsByType(self, classType):
        result_array = []
        for i in self.objects:
            if i.classType == classType:
                result_array.append(i)
        return result_array

    def getClassificationNames(self):
        clist = []
        for o in self.classifications:
            clist.append(o.name)
        return ','.join(clist)

    def getBusinessTermNames(self):
        blist = []
        for o in self.businessterms:
            blist.append(o.name)
        return ','.join(blist)

    def getFriendlyId(self):
        try:
            session = self.session
            originFriendlyName = session.getObjectByLocationID(self.origin).name
            friendlyId = self.externalId.split('~')[0].replace(self.origin, originFriendlyName)
            return friendlyId
        except:
            print("Error getting friendly name session.getObjectByLocationID("+self.origin+").name")

    def getAllRelatedPolicies(self):
        result_array = []
        for obj in self.parentPolicies:
            result_array.append(obj.name)

        for bt in self.businessterms:
            for bt_parentpolicy in bt.parentPolicies:
                result_array.append(bt_parentpolicy.name)

        for cl in self.classifications:
             for cl_parentpolicy in cl.parentPolicies:
                result_array.append(cl_parentpolicy.name)

        for obj in self.child_objects:
            child_array =  obj.getAllRelatedPolicies()
            result_array = result_array + child_array

        ## Remove duplicates
        result_array = list(set(result_array))

        return result_array


    def getParentPolicyNames(self):
        '''
        result_array = []
        for obj in self.parentPolicies:
            result_array.append(obj.name)

        for bt in self.businessterms:
            for bt_parentpolicy in bt.parentPolicies:
                result_array.append(bt_parentpolicy.name)

        for cl in self.classifications:
             for cl_parentpolicy in cl.parentPolicies:
                result_array.append(cl_parentpolicy.name)

        ## Remove duplicates
        result_array = list(set(result_array))
        '''
        result_array = self.getAllRelatedPolicies()
        return ','.join(result_array)

    def __init__(self, session, raw_map ):
        self.classifications = []
        self.businessterms = []
        self.objects = []
        self.parent_objects = []
        self.child_objects = []
        self.session = session
        self.name = raw_map['core.name']
        self.isResource = False
        self.isDataSet = False
        self.isDataElement = False
        try:
            for x in raw_map['type']:
                if x == "core.DataElement":
                    self.isDataElement = True
                if x == "core.DataSet":
                    self.isDataSet = True
        except:
            pass
        self.map = raw_map
        self.description = ""
        try:
            self.description = self.getvalue('core.description')
        except:
            pass
        self.origin = self.getvalue('core.origin')
        self.externalId = self.getvalue('core.externalId')
        self.classType = self.getvalue('core.classType')
        self.shortType = self.classType.split('.')[-1]
        self.elementType = self.getvalue('elementType')
        self.identity = self.getvalue('core.identity')
        self.parentPolicies = []





class INFASession:

    def debug(self, message):
        if debugFlag:
            print(f"DEBUG: {message}")

    def get_sessionid_and_orgid(self, username, password):
        url = self.url_base+'/identity-service/api/v1/Login'
        d = {}
        d['username'] = username
        d['password'] = password
        payload = json.dumps(d)
        headers = {
        'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = response.json()
        session_id = response_data['sessionId']
        org_id = response_data['currentOrgId']

        return session_id, org_id

    def get_token(self,session_id, org_id):
        url = self.url_base+"/identity-service/api/v1/jwt/Token?client_id=cdlg_app&nonce=gxx3t69BWB49BHHNn&access_code="

        payload = ""
        headers = {}
        headers['Content-Type'] = 'application/json'
        headers['IDS-SESSION-ID'] = session_id
        headers['X-INFA-ORG-ID'] =  org_id

        response = requests.request("GET", url, headers=headers, data=payload)
        response_data = response.json()

        token = response_data['jwt_token']
        return token

    def DG_elastic_search(self, json_query):
        url = self.hawk_url_base+"/ccgf-searchv2/api/v1/search"
        payload = json_query
        headers = {}
        headers['Content-Type'] = 'application/json'
        headers['X-INFA-SEARCH-LANGUAGE'] = 'elasticsearch'
        headers['X-INFA-ORG-ID'] =  self.org_id
        headers['Authorization'] =  'Bearer '+self.token

        self.debug(f"idmc_api.DG_elastic_search: About to call {url} Payload: {payload}")

        response = requests.request("POST", url, headers=headers, data=payload)

        self.debug(f"idmc_api.DG_elastic_search: Raw Response for {url}:")
        self.debug(f"{response.text}")
        return response.json()

    def DG_publish(self, json_payload):
        url = self.hawk_url_base+"/ccgf-contentv2/api/v1/publish"
        payload = json_payload
        headers = {}
        headers['Content-Type'] = 'application/json'
        headers['X-INFA-SEARCH-LANGUAGE'] = 'elasticsearch'
        headers['X-INFA-ORG-ID'] =  self.org_id
        headers['Authorization'] =  'Bearer '+self.token

        self.debug("publish: "+json_payload)
        response = requests.request("POST", url, headers=headers, data=payload)
        return response.json()

    def deleteById(self, obj_identity):

        o = self.getObjectByID(obj_identity)
        classType = o.classType
        elementType = o.elementType
        payload = json.dumps({
        "items": [
            {
            "elementType": elementType,
            "identity": obj_identity,
            "operation": "DELETE",
            "type": classType,
            "identityType": "INTERNAL",
            "attributes": {}
            }
        ]
        })

        return self.DG_publish(payload)


    def fetchResources(self):
        payload = json.dumps({
        "from": 0,
        "size": 1000,
        "query": {
            "term": {
            "core.classType": "core.Resource"
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        })

        response_data = self.DG_elastic_search(payload)

        for obj in response_data['hits']['hits']:
            raw_map = obj['sourceAsMap']
            try:
                resource = INFA_DG_Object(self, raw_map)
                resource.isResource = True
                self.resources.append(resource)
            except:
                pass

    def fetchClassifications(self):
        self.fetchParentPolicyOfClassifications()

        payload = json.dumps({
        "from": 0,
        "size": 1000,
        "query": {
            "term": {
            "core.classType": "core.DataElementClassification"
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        })

        response_data = self.DG_elastic_search(payload)

        for obj in response_data['hits']['hits']:
            raw_map = obj['sourceAsMap']
            try:
                classification = INFA_DG_Object(self, raw_map)
                classification.parentPolicies = self.fetchParentPolicyOfClassification(classification.identity)
                self.classifications.append(classification)
                self.all_objects.append(classification)
            except:
                pass

        payload = json.dumps({
        "from": 0,
        "size": 1000,
        "query": {
            "term": {
            "core.classType": "core.DataEntityClassification"
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        })

        response_data = self.DG_elastic_search(payload)

        for obj in response_data['hits']['hits']:
            raw_map = obj['sourceAsMap']
            try:
                classification = INFA_DG_Object(self, raw_map)
                self.classifications.append(classification)
                self.all_objects.append(classification)
            except:
                pass

    def fetchParentPolicyOfClassifications(self):

        result_objects = []
        payload = json.dumps({
        "from": 0,
        "size": 1000,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"elementType": "RELATIONSHIP" }},
                    {"term": {"type":"com.infa.ccgf.models.governance.relatedPolicyClassification" }}
                ]
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        })

        response_data = self.DG_elastic_search(payload)
        search_results = response_data['hits']['hits']
        for res in search_results:
            this_source_identity = res['sourceAsMap']['core.sourceIdentity']
            this_target_identity = res['sourceAsMap']['core.targetIdentity']
            this_relationship = {"name": this_source_identity+" "+this_target_identity, "source_identity": this_source_identity, "target_identity": this_target_identity}
            self.all_relationships.append(this_relationship)


    def fetchParentPolicyOfClassification(self, classification_id):
        result_objects = []
        for rel in self.all_relationships:
            source_identity = rel["source_identity"]
            target_identity = rel["target_identity"]
            if target_identity == classification_id:
                for pol in self.policies:
                    if pol.identity == source_identity:
                        self.debug(f"Adding policy {pol.name} as a parent of Classification with ID of {classification_id}")
                        result_objects.append(pol)

        return result_objects

    def fetchParentPolicyOfBusinessTerms(self):

        result_objects = []
        payload = json.dumps({
        "from": 0,
        "size": 1000,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"elementType": "RELATIONSHIP" }},
                    {"term": {"type":"com.infa.ccgf.models.governance.relatedBusinessTermPolicy" }}
                ]
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        })

        response_data = self.DG_elastic_search(payload)
        search_results = response_data['hits']['hits']
        for res in search_results:
            this_source_identity = res['sourceAsMap']['core.sourceIdentity']
            this_target_identity = res['sourceAsMap']['core.targetIdentity']
            this_relationship = {"name": this_source_identity+" "+this_target_identity, "source_identity": this_source_identity, "target_identity": this_target_identity}
            self.all_relationships.append(this_relationship)


    def fetchParentPolicyOfBusinessTerm(self, business_term_id):

        result_objects = []
        for rel in self.all_relationships:
            source_identity = rel["source_identity"]
            target_identity = rel["target_identity"]
            if target_identity == business_term_id:
                for pol in self.policies:
                    if pol.identity == source_identity:
                        self.debug(f"Adding policy {pol.name} as a parent of Business Term with ID of {business_term_id}")
                        result_objects.append(pol)

        return result_objects

    def fetchPolicies(self):

        payload = json.dumps({
        "from": 0,
        "size": 1000,
        "query": {
            "term": {
            "core.classType": "com.infa.ccgf.models.governance.Policy"
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        })

        response_data = self.DG_elastic_search(payload)

        for obj in response_data['hits']['hits']:
            raw_map = obj['sourceAsMap']
            try:
                pol = INFA_DG_Object(self, raw_map)
                self.policies.append(pol)
                self.all_objects.append(pol)
            except:
                pass

    def fetchBusinessTerms(self):

        self.fetchParentPolicyOfBusinessTerms()

        payload = json.dumps({
        "from": 0,
        "size": 1000,
        "query": {
            "term": {
            "core.classType": "com.infa.ccgf.models.governance.BusinessTerm"
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        })

        response_data = self.DG_elastic_search(payload)

        for obj in response_data['hits']['hits']:
            raw_map = obj['sourceAsMap']
            try:
                term = INFA_DG_Object(self, raw_map)
                term.parentPolicies = self.fetchParentPolicyOfBusinessTerm(term.identity)

                self.businessterms.append(term)
                self.all_objects.append(term)
            except:
                pass

    def getObjectByID(self, identity):


        for o in self.all_objects:
            if o.identity == identity:
                return o


        payload = json.dumps({
        "from": 0,
        "size": 1000,
        "query": {
            "bool": {
            "filter": [
                {
                "term": {
                    "core.identity": identity
                }
                }
            ]
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        })

        response_data = self.DG_elastic_search(payload)

        for obj in response_data['hits']['hits']:
            raw_map = obj['sourceAsMap']
            try:
                o = INFA_DG_Object(self, raw_map)
                self.all_objects.append(o)
                return o
            except:
                pass




    def getObjectByLocationID(self, locationID):
        for o in self.resources:
            if o.origin == locationID and o.isResource:
                return o

            '''
            try:
                location = o.map['core.location']
                if locationID+"://"+locationID == location:
                    return o
            except:
                pass
            '''

    def getObjectByName(self, name):
        for o in self.all_objects:
            if o.name == name:
                return o

    def CDI_getMappingTask(self, mt_ID):
        url = self.pod_url_base+"/saas/api/v2/mttask/frs/"+mt_ID
        headers = {}
        headers['Accept'] =  'application/json'
        headers['icSessionId'] =  self.session_id

        response = requests.request("GET", url, headers=headers)

        return response.json()

    def CDI_createMappingTask(self, payload):
        url = self.pod_url_base+"/saas/api/v2/mttask/"
        headers = {}
        headers['icSessionId'] =  self.session_id
        headers['Content-Type'] = 'application/json'

        response = requests.request("POST", url, headers=headers, data=payload)

        return response.json()

    def CDI_runMappingTask(self, taskId):
        url = self.pod_url_base+"/saas/api/v2/job"
        headers = {}
        headers['icSessionId'] =  self.session_id
        headers['Content-Type'] = 'application/json'
        payload = '{"@type": "job","taskId": "'+taskId+'","taskType": "MTT"}'

        response = requests.request("POST", url, headers=headers, data=payload)

        return response.json()

    def CDI_checkTaskJobLog(self, taskId):
        url = self.pod_url_base+"/saas/api/v2/activity/activityLog"+"?taskId="+taskId
        headers = {}
        headers['icSessionId'] =  self.session_id
        headers['Content-Type'] = 'application/json'

        response = requests.request("GET", url, headers=headers)

        return response.json()

    def CDI_waitForMappingTaskJob(self, taskId):
        jobState = 0
        while jobState != 1 and jobState != 2 and jobState != 3:
            result_json = self.CDI_checkTaskJobLog(taskId)
            temp_state = 0
            for result in result_json:
                temp_state = result['state']
            jobState = temp_state
            time.sleep(5)
        return jobState



    def __init__(self, username,password,url_base=default_infa_url_base, pod_url_base=default_pod_url_base, hawk_url_base=default_infa_hawk_url_base,fetchDGObjects=default_fetch_DG_Objects_On_Connect,debugFlag=debugFlag):
        self.all_relationships = []
        self.all_objects = []
        self.businessterms = []
        self.classifications = []
        self.policies = []
        self.resources = []
        self.url_base = url_base
        self.hawk_url_base = hawk_url_base
        self.pod_url_base = pod_url_base
        self.session_id, self.org_id = self.get_sessionid_and_orgid(username, password)
        if fetchDGObjects:
            self.token = self.get_token(self.session_id, self.org_id)
            ## self.hawk_url_base = 'https://cdgc-api.dm-us.informaticacloud.com'
            print(f"INFO: Fetching Policy Information")
            self.fetchPolicies()
            print(f"INFO: Fetching Resource Information")
            self.fetchResources()
            print(f"INFO: Fetching Classification Information")
            self.fetchClassifications()
            print(f"INFO: Fetching Business Term Information")
            self.fetchBusinessTerms()

def debug():

    target_table_parameter_labels = ["tgt_table"]
    field_mapping_parameter_label = "p_field_mappings"
    container_id_for_new_mappings = "96tlNYKGZPagMafjkrMUqf"
    list_of_acceptable_columns = 'POC_EMAIL,POC_TITLE'

    date_time_obj = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    session = INFASession(username="shayes_pa", password="Infa#2024", url_base=default_infa_url_base, pod_url_base=default_pod_url_base, hawk_url_base=default_infa_hawk_url_base, fetchDGObjects=default_fetch_DG_Objects_On_Connect, debugFlag=debugFlag)
    original_mapping_task = session.CDI_getMappingTask("6E1lFlmdwslj3N9jaA5gA0")
    new_mapping_task = original_mapping_task.copy()

    ## New name for Mapping Task, based on the Original
    new_name = f"{original_mapping_task['name']}_{date_time_obj}"
    new_mapping_task['name'] = new_name

    ## Add a container, to control where the new mapping task will live.
    new_mapping_task['containerId'] = container_id_for_new_mappings

    ## Find the p_field_mappings string parameter
    parameter_mappings = ""
    for param in original_mapping_task['parameters']:
        if param['label'] == field_mapping_parameter_label:
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

    ## Updating the mapping with the new text
    for param in new_mapping_task['parameters']:
        if param['label'] == field_mapping_parameter_label:
            param['text'] = acceptable_mappings_str

    ## Updating the Target table parameter(s)
    create_table_statements = []
    for target in target_table_parameter_labels:
        for param in new_mapping_task['parameters']:
            try:
                if param['uiProperties']['objlabel'] == target:
                    old_name = param['targetObject']
                    new_name = old_name+"_"+date_time_obj
                    param['targetObject'] = new_name
                    param['targetObjectLabel'] = new_name
                    param['objectName'] = new_name
                    param['objectLabel'] = new_name
                    create_statement = f"create table {new_name} as select * from {old_name} where 1=2;"
                    create_table_statements.append(create_statement)
            except:
                pass

    new_mapping_task_json = json.dumps(new_mapping_task, indent = 4)

    print(f"{parameter_mappings}")
    ## print(f"{new_mapping_task_json}")

    new_mapping_task_obj = session.CDI_createMappingTask(new_mapping_task_json)
    new_mapping_task_id = new_mapping_task_obj['id']
    print(f"{create_table_statements}")
    wait = input(f"press any key to run ...")
    session.CDI_runMappingTask(new_mapping_task_id)







'''
session = INFASession('reinvent01', 'infa@1234')

for r in session.resources:
    if r.name == 'Snowflake Emea':
        r.fetchObjects()
        print(r.getvalue('core.name')+' '+r.identity+" "+r.origin+' '+str(len(r.objects)))
        for col in r.getObjectsByShortType('Column'):
            print(str(col.getFriendlyId())+" (Class:"+col.getClassificationNames()+") (Terms: "+col.getBusinessTermNames()+")")

## o = session.getObjectByID('89957f4d-ff7f-4053-9612-1eea9e77e904')
print("DEBUG All Objects Count: "+str(len(session.all_objects)))

print("DEBUG: "+str(session.getObjectByLocationID('a3dd21c4-cd07-357c-9050-858d73220fdc').name))
for o in session.all_objects:
    if o.identity == 'a3dd21c4-cd07-357c-9050-858d73220fdc':
        print("Found that object!")

'''