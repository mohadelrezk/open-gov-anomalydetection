class NetworkComponent:
    DatasetId=""
    publisherId=""
    namedEntities=[]

    def get_namedEntities (self):
        print(self.namedEntities)
        return self.namedEntities

    def getAsJson(self):
        print "to json implementation"



    def NER(docTemp):
        namedEntities=[]
        #Named entities detection logic
        return namedEntities

    def harvestDatasets(self, api):
        #api connection logic
        #entity recognistion function call
        self.namedEntities = self.NER("")
        self.DatasetId = ""
        self.publisherId = ""

    def clear(self):
        self.DatasetId = ""
        self.publisherId = ""
        self.namedEntities = []



class DataSet(NetworkComponent):
    rowData=""
    DatasetId = ""
    publisherId = ""

    title=""
    notes=""
    collection_name=""
    textRepresentation=""


    Stanford_Entities = []
    Stanford_NERrefined=[]

    DBpedia_Entities = []

    def getAsJson(self):
        jsonresponde={}

        #jsonresponde["_id"] = self.DatasetId.encode('hex')
        #jsonresponde["_id"] = self.DatasetId
        jsonresponde["DatasetId"]=self.DatasetId
        jsonresponde["publisherId"] = self.publisherId
        jsonresponde["rowData"] = self.rowData
        jsonresponde["textRepresentation"] = self.textRepresentation
        jsonresponde["Stanford_Entities"] = self.Stanford_Entities
        jsonresponde["Stanford_NERrefined"] = self.Stanford_NERrefined

        jsonresponde["DBpedia_Entities"] = self.DBpedia_Entities

        return jsonresponde

    def constructTextualRepresentation(self):
        TextVector = ""
        try:

            catalogue = self.rowData
            #print catalogue


            if 'collection-name' in catalogue:
                if catalogue['collection-name']:
                    TextVector += ' '+ str(catalogue['collection-name'])
                    #print 'hereee'
                    #print str(catalogue['collection-name'])
            if 'resources' in catalogue:
                if len(catalogue['resources']) > 0:
                    for resource in catalogue['resources']:
                        if 'name' in resource:
                            if resource['name']:
                                TextVector += ' ' + str(resource['name'])
            if 'tags' in catalogue:
                if len(catalogue['tags'])>0:
                    for tag in catalogue['tags']:
                        if 'display_name' in tag:
                            if tag['display_name']:
                                TextVector += ' ' + str(tag['display_name'])
            if 'organization' in catalogue:
                if 'description' in catalogue['organization']:
                    if catalogue['organization']['description'] :
                        TextVector += ' ' + str(catalogue['organization']['description'])
                if 'title' in catalogue['organization']:
                    if catalogue['organization']['title']:
                        TextVector += ' ' + str(catalogue['organization']['title'])
            if 'contact-name' in catalogue:
                if catalogue['contact-name']:
                    TextVector += ' ' + str(catalogue['contact-name'])
            if 'notes' in catalogue:
                if catalogue['notes']:
                    TextVector += ' ' + str(catalogue['notes'])
            if 'individual_resources' in catalogue:
                if len(catalogue['individual_resources']) > 0:
                    for resource in catalogue['individual_resources']:
                        if 'name' in resource:
                            if resource['name']:
                                TextVector += ' ' + str(resource['name'])
            if 'title' in catalogue:
                if catalogue['title']:
                    TextVector += ' ' + str(catalogue['title'])

            if self.publisherId:
                TextVector += ' ' + self.publisherId
            if self.DatasetId:
                TextVector += ' ' + self.DatasetId

        except Exception, e:
            print 'constructTextualRepresentation'
            print e

        self.textRepresentation = TextVector
        print 'TextVector'
        print TextVector
        return TextVector

class Publisher(NetworkComponent):
    publisherId = ""
    Entities = []

class Relation(NetworkComponent):

    datasetA = ""
    datasetB = ""
    publisherA = ""
    publisherB = ""
    commonsEntities = []
    relationStrength = 0
    vectorA=[]
    vectorB=[]


    def getAsJson(self):

        jsonresponde = {}

        # jsonresponde["_id"] = self.DatasetId.encode('hex')
        # jsonresponde["_id"] = self.DatasetId
        jsonresponde["datasetA"] = self.datasetA
        jsonresponde["datasetB"] = self.datasetB
        jsonresponde["publisherA"] = self.publisherA
        jsonresponde["publisherB"] = self.publisherB
        jsonresponde["commonsEntities"] = self.commonsEntities
        jsonresponde["relationStrength"] = len(self.commonsEntities)
        jsonresponde["vectorA"] = self.vectorA
        jsonresponde["vectorB"] = self.vectorB

        return jsonresponde


    def clear(self):

        self.datasetA = ""
        self.datasetB = ""
        self.publisherA = ""
        self.publisherB = ""
        del self.commonsEntities [:]
        self.relationStrength = 0
