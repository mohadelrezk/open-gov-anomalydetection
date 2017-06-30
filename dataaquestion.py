from skelton import DataSet, Relation
from mongo import Mongo
import requests
import json
from time import sleep
from loggiingg import logee

class DataAquestion:



    # initiate logger object
    global log
    log = logee("collaboration.log", "rtpa-network-builder-dataaquestion_py-DataAquestion")


    global dataset
    dataset = DataSet()
    global mongo
    mongo = Mongo()
    global relation
    relation = Relation()

    def __init__(self, dataportal, mongodomain, mongoport, dbname, datasetsIdsCollection, datasetsCollection):

        global dataPortal
        global mongoDomain
        global mongoPort  # must be integer
        global dbName
        global dataSetsIdsCollection
        global dataSetsCollection

        dataPortal = dataportal
        mongoDomain = mongodomain
        mongoPort = mongoport  # must be integer
        dbName = dbname
        dataSetsIdsCollection = datasetsIdsCollection
        dataSetsCollection = datasetsCollection

    def constructIdsList(self):  # run only one time for every ckan portal

        print dataPortal
        print mongoDomain
        print mongoPort  # must be integer
        print dbName
        print dataSetsIdsCollection
        print dataSetsCollection

        # 1 Connect to API

        # getdatasetsIds = "http://data.gov.ie/api/3/action/package_list"
        # getDataSetMetaData = "http://data.gov.ie/api/3/action/package_show?id="
        getdatasetsIds = dataPortal + "/api/3/action/package_list"

        datasetsIdsRequest = requests.get(getdatasetsIds)

        datasetsIdsJson = datasetsIdsRequest.json()

        datasetsIdsList = []

        for datasetID in datasetsIdsJson["result"]:

            # logging to service.log and to screen
            log.logger.info("datasetIds (datasetID)")
            log.logger.info(str(datasetID))

            datasetsIdsList.append(datasetID)

        # Append data to mongo
        idsDic = {}
        idsDic["datasetIds"] = datasetsIdsList

        collec = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsIdsCollection)
        mongo.appendDataListToMongo(collec, idsDic)

    def harvestDatasets(self, startFrom):
        CounterWhereAmI = startFrom
        try:
            # 1 Connect to API

            getDataSetMetaData = dataPortal + "/api/3/action/package_show?id="

            # release memory
            dataset.clear()

            # 2 Collect Data
            # get dataset list from mongo

            datasetListcollecion = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsIdsCollection)
            datasetListcursor = mongo.getfromMongo(datasetListcollecion, '')

            # cur_dic= cursor.to_dict()
            # datasetsIdsList = cur_dic['datasetIds']
            # print datasetsIdsList

            for object in datasetListcursor:
                # print ob
                datasetsIdsList = object['datasetIds']

            datasetListcursor.close()

            print CounterWhereAmI

            for dataSetindex in range(CounterWhereAmI - 1, len(datasetsIdsList)):
                dataSetMetaDataRequest = requests.get(str(getDataSetMetaData) + str(datasetsIdsList[dataSetindex]))
                dataSetMetaDataJson = dataSetMetaDataRequest.json()
                print json.dumps(dataSetMetaDataJson)
                dataset.DatasetId = datasetsIdsList[dataSetindex]

                if "result" in dataSetMetaDataJson:

                    if "owner_org" in dataSetMetaDataJson["result"]:
                        if dataSetMetaDataJson["result"]["owner_org"]:
                            dataset.publisherId = dataSetMetaDataJson["result"]["owner_org"]

                    elif "organization" in dataSetMetaDataJson["result"]:
                        if "id" in dataSetMetaDataJson["result"]["organization"]:
                            if dataSetMetaDataJson["result"]["organization"]["id"]:
                                dataset.publisherId = dataSetMetaDataJson["result"]["organization"]["id"]

                    elif "publisherId" in dataSetMetaDataJson["result"]:
                        if dataSetMetaDataJson["result"]["publisherId"]:
                            dataset.publisherId = dataSetMetaDataJson["result"]["publisherId"]

                    elif "author" in dataSetMetaDataJson["result"]:
                        if dataSetMetaDataJson["result"]["author"]:
                            dataset.publisherId = dataSetMetaDataJson["result"]["author"]

                    elif "author_email" in dataSetMetaDataJson["result"]:
                        if dataSetMetaDataJson["result"]["author_email"]:
                            dataset.publisherId = dataSetMetaDataJson["result"]["author"]
                    else:
                        dataset.publisherId = "not found!"

                dataset.rowData = dataSetMetaDataJson["result"]
                dataset.constructTextualRepresentation()


                # create josn out of data set

                datasetAsDic = dataset.getAsJson()

                # logging to service.log and to screen
                log.logger.info("datasetAsDic")
                log.logger.info(datasetAsDic)



                """
                for feature in dataSetMetaDataJson["result"]:
                    print feature
                    datasetsIdsList.append(datasetID)
                """

                # Push Data set to Mongo
                datesetscollection = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsCollection)
                # obId = mongo.appendToMongo(collec, datasetAsDic)
                mongo.appendDataSetToMongo(datesetscollection, datasetAsDic)
                # print obId


                # Counter, sleeping for API and releasing memory
                CounterWhereAmI = CounterWhereAmI + 1


                # logging to service.log and to screen
                log.logger.info("CounterWhereAmI")
                log.logger.info(CounterWhereAmI)

                dataset.clear()
                sleep(0.5)

        except BaseException as exce:

            # logging to service.log and to screen
            log.logger.error("def harvestDatasets(self,startFrom, NER)")
            log.logger.error(exce)
            log.logger.error(CounterWhereAmI)

            sleep(0.5)
            self.harvestDatasets(CounterWhereAmI)

        return CounterWhereAmI