


from dbpedia_spotlight import Dbpedia
from stanfordNER import StanfordNER
from mongo import Mongo
from skelton import DataSet
from time import sleep
from loggiingg import logee



class TextAnalysis:





    def __init__(self, dataportal, mongodomain, mongoport, dbname, datasetsIdsCollection, datasetsCollection):


        global dataPortal
        global mongoDomain
        global mongoPort  # must be integer
        global dbName
        global dataSetsIdsCollection
        global dataSetsCollection

        dataPortal = dataportal
        mongoDomain = mongodomain
        mongoPort  = mongoport # must be integer
        dbName = dbname
        dataSetsIdsCollection = datasetsIdsCollection
        dataSetsCollection = datasetsCollection

        # initiate logger object
        global log
        log = logee("collaboration.log", "rtpa-network-builder-textAnalysis_py-TextAnalysis")

        global stanford
        stanford = StanfordNER()

        global dbpedia
        dbpedia = Dbpedia()

        global mongo
        mongo = Mongo()

        global dataset
        dataset = DataSet()


    def analyze(self, startFrom):

        # release memory
        dataset.clear()

        #get datasets from mongo

        CounterWhereAmI = startFrom
        try:

            # 2 Collect Data
            # get dataset list from mongo

            datasetListcollecion = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsIdsCollection)
            #print mongoDomain, mongoPort, dbName, dataSetsIdsCollection
            datasetListcursor = mongo.getfromMongo(datasetListcollecion, '')


            for object in datasetListcursor:
                # print ob
                datasetsIdsList = object['datasetIds']

            datasetListcursor.close()


            print CounterWhereAmI

            for dataSetindex in range(CounterWhereAmI - 1, len(datasetsIdsList)):

                #get data from mongo
                q = {"_id":""}
                q ["_id"] = str(datasetsIdsList[dataSetindex])

                # logging to service.log and to screen
                log.logger.info("analyze query")
                log.logger.info(q)

                datesetscollection = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsCollection)
                cursuer = mongo.getfromMongoFindone(datesetscollection,q)

                dataset.DatasetId = cursuer.get('_id')#str(datasetsIdsList[dataSetindex])
                dataset.publisherId = cursuer.get('publisherId')
                dataset.rowData = cursuer.get('rowData')

                """important NER part"""
                self.analyze_dataset_representation_stanf(dataset)
                self.analyze_dataset_representation_dbpedia(dataset)

                datasetAsDic = dataset.getAsJson()

                # logging to service.log and to screen
                log.logger.info("datasetAsDic")
                log.logger.info(datasetAsDic)

                """
                for feature in dataSetMetaDataJson["result"]:
                    print feature
                    datasetsIdsList.append(datasetID)
                """

                # Push whole Relation to Mongo
                textAnalysisCollectionName = dataSetsCollection + '_AfterTextAnalysis'

                # Push Data set to Mongo again after TEXTUAL Analysis
                datesetscollection = mongo.connect(mongoDomain, mongoPort, dbName, textAnalysisCollectionName)
                # obId = mongo.appendToMongo(collec, datasetAsDic)
                mongo.appendDataSetToMongo(datesetscollection, datasetAsDic)
                # print obId


                # Counter, sleeping for API and releasing memory
                CounterWhereAmI = CounterWhereAmI + 1


                # logging to service.log and to screen
                log.logger.info("CounterWhereAmI")
                log.logger.info(CounterWhereAmI)

                dataset.clear()
                #sleep(0.5)

        except BaseException as exce:


            # logging to service.log and to screen
            log.logger.error("def analyze(self,startFrom, NER):")
            log.logger.error(exce)
            log.logger.error(CounterWhereAmI)

            #sleep(0.5)
            self.analyze(CounterWhereAmI)

        return CounterWhereAmI



    def analyze_dataset_representation_stanf(self, dataset):
            """STANFORD"""
            stanford.NER(dataset)

    def analyze_dataset_representation_dbpedia(self, dataset):
        """DBpedia Spotlight"""
        dbpedia.get_dbpedia_NER(dataset)

