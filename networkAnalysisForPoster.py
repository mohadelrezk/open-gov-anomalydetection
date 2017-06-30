from mongo import Mongo
import csv
import copy
from loggiingg import logee


class NetworkAnalysis:







    def __init__(self, dataportal, mongodomain, mongoport, dbname, datasetsIdsCollection, datasetsCollection, outputFolder, comparsionfield):

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

        # initiate logger object
        global log
        log = logee("collaboration.log", "rtpa-network-builder-NetworkAnalysisForPoster_py-NetworkAnalysis")

        # connect
        global mongo
        mongo = Mongo()

        # csv files output location
        global folder
        folder = outputFolder#"poster/"

        # unique comparison field
        global comparsionfieldd
        comparsionfieldd = comparsionfield


    #get relations in csv
    def relationTable(self):
        #datasetcollecion = mongo.connect('localhost', 27017, 'rtpa', 'datagovie_1_PubllishersNetwork')
        publishersCollectionName = dataSetsCollection + '_PubllishersNetwork_CF_'+comparsionfieldd
        datasetcollecion = mongo.connect(mongoDomain, mongoPort, dbName, publishersCollectionName)


        datasetpublisherCursor = mongo.getfromMongo(datasetcollecion, "").sort("relationStrength", -1)

        header = ['publisherA', 'publisherB', 'stregnth', 'commons']
        relationrecord_r = []
        with open(folder+"CF_"+comparsionfieldd+"_"+"relation.csv", 'w+') as output:
            outputwriter = csv.writer(output, delimiter=',', quotechar='\"')
            # putting the header
            outputwriter.writerow(header)

            commons = []
            for relationrecord in datasetpublisherCursor:
                # print relationrecord
                relationrecord_r.append(relationrecord['publisherA'])
                relationrecord_r.append(relationrecord['publisherB'])
                relationrecord_r.append(relationrecord['relationStrength'])

                for e in relationrecord['commonsEntities']:
                    t = (e["type"].encode('utf-8'), e['entity'].encode('utf-8'))
                    commons.append(copy.deepcopy(t))
                relationrecord_r.append(copy.deepcopy(commons))
                print relationrecord_r

                outputwriter.writerow(copy.deepcopy(relationrecord_r))

                del relationrecord_r[:]
                del commons[:]
                del t

    #get top entities frequency in all datasets
    def topentities(self):
        #datasetcollecion = mongo.connect('localhost', 27017, 'rtpa', 'datagovie_2')
        finalstageCollectionName = dataSetsCollection + "_After3gramRefinment" #_After3gramRefinment
        datasetcollecion = mongo.connect(mongoDomain, mongoPort, dbName, finalstageCollectionName)


        fieldName = "$"+comparsionfieldd # '$Entities'
        fieldName_entity = fieldName + '.entity' # '$Entities.entity'

        q= [{"$unwind": fieldName},{"$group": {"_id": fieldName_entity, "count": {"$sum": 1}}},{"$sort" : { "count" : -1} }]

        Cursor = datasetcollecion.aggregate(q)

        header = ['entity', 'count']
        record_r = []
        with open(folder+"CF_"+comparsionfieldd+"_"+"entitycount.csv", 'w+') as output:
            outputwriter = csv.writer(output, delimiter=',', quotechar='\"')
            # putting the header
            outputwriter.writerow(header)

            commons = []
            for record in Cursor:
                # print relationrecord
                record_r.append(record['_id'])
                print record['_id']
                record_r.append(record['count'])            #record_r.append(relationrecord['relationStrength'])
                print record['count']
                #print record_r
                import copy
                outputwriter.writerow(copy.deepcopy(record_r))
                del record_r[:]


    #get top types frequency in all datasets
    def toptypes(self):

        #mongo connection
        finalstageCollectionName = dataSetsCollection + "_After3gramRefinment" #_After3gramRefinment
        datasetcollecion = mongo.connect(mongoDomain, mongoPort, dbName, finalstageCollectionName)

        fieldName = "$" + comparsionfieldd  # '$Entities'
        fieldName_type = fieldName + '.type'  # '$Entities.type'

        q= [{"$unwind": fieldName},{"$group": {"_id": fieldName_type, "count": {"$sum": 1}}},{"$sort" : { "count" : -1} }]

        Cursor = datasetcollecion.aggregate(q)

        header = ['type', 'count']
        record_r = []
        with open(folder+"CF_"+comparsionfieldd+"_"+"typecount.csv", 'w+') as output:
            outputwriter = csv.writer(output, delimiter=',', quotechar='\"')
            # putting the header
            outputwriter.writerow(header)

            commons = []
            for record in Cursor:
                # print relationrecord
                record_r.append(record['_id'])
                print record['_id']
                record_r.append(record['count'])            #record_r.append(relationrecord['relationStrength'])
                print record['count']
                #print record_r
                import copy
                outputwriter.writerow(copy.deepcopy(record_r))
                del record_r[:]


    #top entitis by top publisher
    def topentitiesBypublidhers(self):
        header = ['publisher', 'entities;count']
        record_r = []
        with open(folder+"CF_"+comparsionfieldd+"_"+"entitycountbypublisher.csv", 'w+') as output:
            outputwriter = csv.writer(output, delimiter=',', quotechar='\"')
            # putting the header
            outputwriter.writerow(header)

        q_top_pub =[{"$group": {"_id": '$publisherId', "count": {"$sum": 1}}},
        {"$sort" : { "count" : -1} },
        { "$limit" : 20 }]

        # mongo connection
        finalstageCollectionName = dataSetsCollection + "_After3gramRefinment"  # _After3gramRefinment
        datasetcollecion = mongo.connect(mongoDomain, mongoPort, dbName, finalstageCollectionName)

        #Cursor = mongo.getdistinctfromMongo(datasetcollecion,"publisherId","")
        Cursor = datasetcollecion.aggregate(q_top_pub)

        for pub in Cursor:
            publisher=  pub["_id"]
            print publisher

            fieldName = "$" + comparsionfieldd  # '$Entities'
            fieldName_entity = fieldName + '.entity'  # '$Entities.entity'

            q= [{"$match":{"publisherId":publisher}},{"$unwind": fieldName},
                {"$group": {"_id": fieldName_entity, "count": {"$sum": 1}}},{"$sort" : { "count" : -1} },{ "$limit" : 20 }]

            Cursor = datasetcollecion.aggregate(q)


            with open(folder+"CF_"+comparsionfieldd+"_"+"entitycountbypublisher.csv", 'a+') as output:
                outputwriter = csv.writer(output, delimiter=',', quotechar='\"')
                record_r.append(publisher)
                entities=""
                c =list(Cursor)
                #for record in c:
                for i in xrange(0,len(c)):
                    # print relationrecord
                    entities += str(c[i]['_id'])
                    entities += ";"
                    entities += str(c[i]['count'])
                    if i < len(c)-1:
                        entities += ", "

                    #print record['_id']
                    #record_r.append(record['count'])  # record_r.append(relationrecord['relationStrength'])
                    #print record['count']
                    # print record_r
                    #import copy
                record_r.append(entities)
                outputwriter.writerow(copy.deepcopy(record_r))
                del record_r[:]



    def run (self):

        """ RUN
        relationTable()
        topentities()
        toptypes()
        topentitiesBypublidhers()
        """

        self.relationTable()
        self.topentities()
        self.toptypes()
        self.topentitiesBypublidhers()