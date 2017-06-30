from time import sleep

from skelton import DataSet, Relation
from mongo import Mongo
import requests
import json
import itertools
from pymongo import CursorType

from loggiingg import logee


class CollaborationAnalysis():
    global dataset
    dataset = DataSet()
    global mongo
    mongo = Mongo()
    global relation
    relation = Relation()

    # initiate logger object
    global log
    log = logee("collaboration.log", "rtpa-network-builder-CollaborationAnalysis_py-CollaborationAnalysis")


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

    def buildDataSetNetwork(self, comparisonField):
        try:
            #getting nChooseK for datasets (nC2)


            datasetListcollecion = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsIdsCollection)
            datasetListcursor = mongo.getfromMongo(datasetListcollecion, '')


            for object in datasetListcursor:
                # print ob
                datasetsIdsList = object['datasetIds']

            # logging to service.log and to screen
            log.logger.info("datasetsIdsList")
            log.logger.info(datasetsIdsList)


            # getting nChooseK for datasets (nC2)

            nChoose2Fordatasets= list(itertools.combinations(datasetsIdsList, 2))

            # logging to service.log and to screen
            log.logger.info("nChoose2Fordatasets")
            log.logger.info(nChoose2Fordatasets)


            #for combination in nChoose2Fordatasets:
            for i in xrange(0,len(nChoose2Fordatasets)):

                returned_message = self.get_commonEntitiesDatasets(nChoose2Fordatasets[i][0],nChoose2Fordatasets[i][1], comparisonField)

                # logging to service.log and to screen
                log.logger.info("returned_message")
                log.logger.info(returned_message)

                if 'strength' in returned_message:
                    if returned_message['strength'] > 0:
                        sleep(0.5)
                        relation.datasetA = str(nChoose2Fordatasets[i][0])
                        relation.datasetB = str(nChoose2Fordatasets[i][1])
                        # print nChoose2Fordatasets[i][0]
                        # print nChoose2Fordatasets[i][1]
                        relation.publisherA = returned_message ['publisherA']
                        #print relation.publisherA
                        relation.publisherB = returned_message['publisherB']
                        #print relation.publisherB
                        relation.vectorA = returned_message['vectorA']
                        relation.vectorB = returned_message['vectorB']
                        relation.commonsEntities = returned_message['commonsEntities']
                        #print relation.commonsEntities
                        # Push whole Relation to Mongo
                        netwokCollectionName = dataSetsCollection+'_DatasetsNetwork'+"_CF_"+comparisonField
                        #print netwokCollectionName

                        datesetscollection = mongo.connect(mongoDomain, mongoPort, dbName, netwokCollectionName)

                        # logging to service.log and to screen
                        log.logger.info("relation.getAsJson()")
                        log.logger.info(relation.getAsJson())

                        #mm = json.dumps()
                        #print mm
                        mongo.appendRelationToMongo(datesetscollection, relation.getAsJson())
                        relation.clear()
                        #print relation.getAsJson()


            relation.clear()
            #datasetListcursor.close()
        except Exception,e:

            # logging to service.log and to screen
            log.logger.error("def buildDataSetNetwork(self):")
            log.logger.error(e)

    def get_commonEntitiesDatasets(self, NetworkComponentA, NetworkComponentB, comparisonField):
        """ A network component could be PUBLISHER OR DATASET """
        """code effenciency can be increased by storing vectors instead querying it every time"""
        try:

            returned_message = {}
            commonsVector = []
            publisherA = ""
            publisherB = ""
            # query_dic = {}
            vectorA = []
            vectorB = []
            # strength=0

            # get datasets from mongo

            # print DatasetA

            # get diticit publishers list
            # distitinctpublisherFieldA =
            field = comparisonField#"NERrefined"
            query = {}
            # first publiser

            if NetworkComponentA:


                # logging to service.log and to screen
                log.logger.info("NetworkComponentA")
                log.logger.info(NetworkComponentA)

                # publisherA=NetworkComponentA
                query["DatasetId"] = NetworkComponentA

                # print query_dic

                datasetcollecion = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsCollection)
                distinctDatasetcursor = mongo.getdistinctfromMongo(datasetcollecion, field, query)

                # print str(datasecursor.next())


                NetworkComponentA_json = distinctDatasetcursor

                # print "NetworkComponentA_json"
                # print NetworkComponentA_json

                # print DatasetA_json
                # print 'ok'

                if NetworkComponentA_json:
                    # getting refined NER List

                    # if 'NERrefined' in DatasetA_json:
                    #   if len(DatasetA_json['NERrefined']) > 0:
                    # vectorA = list(NetworkComponentA_json) #may need jsom.dumps()
                    vectorA_full = NetworkComponentA_json  # may need jsom.dumps()


                    # logging to service.log and to screen
                    log.logger.info("vectorA_full")
                    log.logger.info(vectorA_full)

                    # getting publisher name

                    # if 'publisherId' in DatasetA_json:
                    #   if DatasetA_json['publisherId']:
                # publisherA = str(DatasetA_json['publisherId'])

                # closing cursor and clearing query
                # distinctpublisherecursor.close()
                query.clear()

                # second publusher

                if NetworkComponentB:
                    print NetworkComponentB
                    # publisherB = NetworkComponentB
                    query["DatasetId"] = NetworkComponentB

                    datasetcollecion = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsCollection)
                    distinctpublisherecursor = mongo.getdistinctfromMongo(datasetcollecion, field, query)

                    NetworkComponentB_json = distinctpublisherecursor


                    # logging to service.log and to screen
                    log.logger.info("NetworkComponentB_json")
                    log.logger.info(NetworkComponentB_json)

                    # print 'ok'
                    if NetworkComponentB_json:
                        # getting refined NER List

                        # if 'NERrefined' in DatasetB_json:
                        #   if len(DatasetB_json['NERrefined']) > 0:
                        # vectorB = list(NetworkComponentB_json) #may need jsom.dumps()
                        vectorB_full = NetworkComponentB_json


                        # logging to service.log and to screen
                        log.logger.info("vectorB_full")
                        log.logger.info(vectorB_full)

                        # getting publisher name
                        # if 'publisherId' in DatasetB_json:
                        #    if DatasetB_json['publisherId']:
                        #       publisherB = str(DatasetB_json['publisherId'])

                    # closing cursor and clearing query
                    # distinctpublisherecursor.close()
                    query.clear()

                    # comparison logic
                    # print vectorA
                    # print vectorB

                    #dictionary to hold entities types
                    hold_types={}

                    # remove NER type tags (org etc..)

                    for entity in vectorA_full:
                        vectorA.append(entity['entity'])
                        hold_types[entity['entity']]=entity['type']

                    for entity in vectorB_full:
                        vectorB.append(entity['entity'])
                        hold_types[entity['entity']] = entity['type']

                    if len(vectorA) > 0 and len(vectorB) > 0:
                        commonsVector_list = list(set(vectorA) & set(vectorB))

                    #new commons list contatining the type
                    commonsVector=[]

                    for e in commonsVector_list:
                        commonsVector.append({'entity':e,'type':hold_types[e]})



                    # logging to service.log and to screen
                    log.logger.info("commonsVector")
                    log.logger.info(commonsVector)

                    # returned message preparation
                    returned_message['commonsEntities'] = commonsVector

                    #get d1 publisher

                    #db.rtpa_dublin_tet_4.distinct("publisherId",{"DatasetId":"prato"})
                    query["DatasetId"]=NetworkComponentA
                    datasetcollecion = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsCollection)
                    datasetpublisherCursor = mongo.getdistinctfromMongo(datasetcollecion, "publisherId", query)

                    returned_message['publisherA'] = str(datasetpublisherCursor[0])
                    query.clear()

                    # get d2 publisher

                    query["DatasetId"] = NetworkComponentB
                    datasetcollecion = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsCollection)
                    datasetpublisherCursor = mongo.getdistinctfromMongo(datasetcollecion, "publisherId", query)

                    returned_message['publisherB'] = str(datasetpublisherCursor[0])

                    returned_message['vectorA'] = vectorA
                    returned_message['vectorB'] = vectorB

                    returned_message['strength'] = len(commonsVector)




        except Exception, e:

            # logging to service.log and to screen
            log.logger.error("def get_commonEntitiesPublishers(self, DatasetA, DatasetB):")
            log.logger.error(e)



        # returning results
        # print returned_message
        return returned_message

        # releasing memory
        del commonsVector[:]
        publisherA = ""
        publisherB = ""
        # query_dic.clear()
        del vectorA[:]
        del vectorB[:]
        hold_types.clear()

    def buildPublisherNetwork(self,comparisonField):
        try:
            #getting nChooseK for publishers (nC2)

            distinctPublisherfield = "publisherId"
            dataSetsCollectionTogetFrom = dataSetsCollection + "_After3gramRefinment"
            datesetscollection = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsCollectionTogetFrom)
            distinctpublisherscursor = mongo.getdistinctfromMongo(datesetscollection,distinctPublisherfield,"")

            #print distinctpublisherscursor

            #for object in distinctpublisherscursor:
                # print ob
            distinctpublishersList = distinctpublisherscursor

            #print distinctpublishersList

            # getting nChooseK for datasets (nC2)

            nChoose2Forpublishers= list(itertools.combinations(distinctpublishersList, 2))
            #print nChoose2Forpublishers

            #for combination in nChoose2Fordatasets:
            for i in xrange(0,len(nChoose2Forpublishers)):

                returned_message = self.get_commonEntitiesPublishers(nChoose2Forpublishers[i][0],nChoose2Forpublishers[i][1], comparisonField)


                # logging to service.log and to screen
                log.logger.info("returned_message")
                log.logger.info(returned_message)

                if 'strength' in returned_message:
                    print returned_message['strength']
                    if returned_message['strength'] > 0:
                        sleep(0.5)
                        #relation.datasetA = str(nChoose2Fordatasets[i][0])
                        #relation.datasetB = str(nChoose2Fordatasets[i][1])
                        # print nChoose2Fordatasets[i][0]
                        # print nChoose2Fordatasets[i][1]
                        relation.publisherA = str(nChoose2Forpublishers[i][0])
                        #print relation.publisherA
                        relation.publisherB = str(nChoose2Forpublishers[i][1])
                        #print relation.publisherB
                        relation.vectorA = returned_message['vectorA']
                        relation.vectorB = returned_message['vectorB']
                        relation.commonsEntities = returned_message['commonsEntities']
                        #print relation.commonsEntities
                        # Push whole Relation to Mongo
                        netwokCollectionName = dataSetsCollection+'_PubllishersNetwork'+"_CF_"+comparisonField
                        #print netwokCollectionName

                        datesetscollection = mongo.connect(mongoDomain, mongoPort, dbName, netwokCollectionName)


                        # logging to service.log and to screen
                        log.logger.info("relation.getAsJson()")
                        log.logger.info(relation.getAsJson())

                        #mm = json.dumps()
                        #print mm
                        mongo.appendPublishersRelationToMongo(datesetscollection, relation.getAsJson())
                        relation.clear()
                        #print relation.getAsJson()


            relation.clear()
            #distinctpublisherscursor.close()
        except Exception,e:

            # logging to service.log and to screen
            log.logger.error("def buildpubisherNetwork(self):")
            log.logger.error(e)

    def get_commonEntitiesPublishers(self, NetworkComponentA, NetworkComponentB, comparisonField):
        """ A network component could be PUBLISHER OR DATASET """
        """code effenciency can be increased by storing vectors instead querying it every time"""
        try:

            returned_message = {}
            commonsVector = []
            publisherA = ""
            publisherB = ""
            #query_dic = {}
            vectorA = []
            vectorB = []
            # strength=0

            # get datasets from mongo

            # print DatasetA

            # get diticit publishers list
            #distitinctpublisherFieldA =
            field = comparisonField #"NERrefined"
            query = {}
#first publiser

            if NetworkComponentA:

                # logging to service.log and to screen
                log.logger.info("NetworkComponentA")
                log.logger.info(NetworkComponentA)

                #publisherA=NetworkComponentA
                query["publisherId"] = NetworkComponentA


            # print query_dic

                dataSetsCollectionTogetFrom = dataSetsCollection + "_After3gramRefinment"
                datasetcollecion = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsCollectionTogetFrom)
                distinctpublisherecursor = mongo.getdistinctfromMongo(datasetcollecion, field,query)

            # print str(datasecursor.next())


                NetworkComponentA_json = distinctpublisherecursor

                #print "NetworkComponentA_json"
                #print NetworkComponentA_json

                # print DatasetA_json
                # print 'ok'

                if NetworkComponentA_json:
                    # getting refined NER List

                    #if 'NERrefined' in DatasetA_json:
                     #   if len(DatasetA_json['NERrefined']) > 0:
                    #vectorA = list(NetworkComponentA_json) #may need jsom.dumps()
                    vectorA_full = NetworkComponentA_json #may need jsom.dumps()


                    # logging to service.log and to screen
                    log.logger.info("vectorA_full")
                    log.logger.info(vectorA_full)

                    # getting publisher name

                    #if 'publisherId' in DatasetA_json:
                     #   if DatasetA_json['publisherId']:
                #publisherA = str(DatasetA_json['publisherId'])

                # closing cursor and clearing query
                #distinctpublisherecursor.close()
                query.clear()

#second publusher

                if NetworkComponentB:
                    print NetworkComponentB
                   # publisherB = NetworkComponentB
                    query["publisherId"] = NetworkComponentB

                    dataSetsCollectionTogetFrom = dataSetsCollection + "_After3gramRefinment"
                    datasetcollecion = mongo.connect(mongoDomain, mongoPort, dbName, dataSetsCollectionTogetFrom)
                    distinctpublisherecursor = mongo.getdistinctfromMongo(datasetcollecion, field, query)

                    NetworkComponentB_json = distinctpublisherecursor


                    # logging to service.log and to screen
                    log.logger.info("NetworkComponentB_json")
                    log.logger.info(NetworkComponentB_json)

                    # print 'ok'
                    if NetworkComponentB_json:
                        # getting refined NER List

                        #if 'NERrefined' in DatasetB_json:
                         #   if len(DatasetB_json['NERrefined']) > 0:
                        #vectorB = list(NetworkComponentB_json) #may need jsom.dumps()
                        vectorB_full = NetworkComponentB_json

                        # logging to service.log and to screen
                        log.logger.info("vectorB_full")
                        log.logger.info(vectorB_full)

                        # getting publisher name
                       # if 'publisherId' in DatasetB_json:
                        #    if DatasetB_json['publisherId']:
                         #       publisherB = str(DatasetB_json['publisherId'])

                    # closing cursor and clearing query
                    #distinctpublisherecursor.close()
                    query.clear()

                    # comparison logic
                    # print vectorA
                    # print vectorB

                    # dictionary to hold entities types
                    hold_types = {}

                    #remove NER type tags (org etc..)

                    for entity in vectorA_full:
                        vectorA.append(entity['entity'])
                        hold_types[entity['entity']] = entity['type']

                    for entity in vectorB_full:
                        vectorB.append(entity['entity'])
                        hold_types[entity['entity']] = entity['type']

                    if len(vectorA) > 0 and len(vectorB) > 0:
                        commonsVector_list = list(set(vectorA) & set(vectorB))


                    # new commons list contatining the type
                    commonsVector = []

                    for e in commonsVector_list:
                        commonsVector.append({'entity':e, 'type':hold_types[e]})


                    # logging to service.log and to screen
                    log.logger.info("commonsVector")
                    log.logger.info(commonsVector)

                    # returned message preparation
                    returned_message['commonsEntities'] = commonsVector

                    #returned_message['publisherA'] = publisherA
                    #returned_message['publisherB'] = publisherB
                    returned_message['vectorA'] = vectorA
                    returned_message['vectorB'] = vectorB

                    returned_message['strength'] = len(commonsVector)




        except Exception, e:


            # logging to service.log and to screen
            log.logger.error("def get_commonEntitiesPublishers(self, DatasetA, DatasetB):")
            log.logger.error(e)

        # returning results
        # print returned_message
        return returned_message

        # releasing memory
        del commonsVector[:]
        publisherA = ""
        publisherB = ""
        #query_dic.clear()
        del vectorA[:]
        del vectorB[:]
        hold_types.clear()