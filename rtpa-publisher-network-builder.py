
from flask import Flask, request, send_file, send_from_directory
from flask_cors import CORS

import json

from loggiingg import logee
import gc
import traceback

from mongo import Mongo

global message
messages =[]

global mongo
mongo = Mongo()


app = Flask(__name__, static_url_path='/static')
CORS(app)

# initiate logger object
global log
log = logee("collaborationWebService.log", "rtpa-network-builder-WebService_py-WebService")


global db
db = "collaborationAnalysisFINAL"

global coll
coll = "datagovie_Final_PubllishersNetwork_CF_DBpedia_Entities"

global relstren
relstren = 20

#garbage collector
gc.enable()


@app.route('/buildRelatednessNet/bypublisher/check')
def checkiflive():
    return "I am live !"

#dbname and collection (relations collection) name expected here
#http://localhost:5001/buildRelatednessNet/getrelations?dbname=rtpa&collection=rtpaG_PubllishersNetwork7
@app.route("/buildRelatednessNet/getrelations")#, methods=['POST'])
def getrelations():
    response_data = {}
    # response_results=json.dumps(response_results_list)
    response_data['result'] = []


    try:
        #logger.debug('route : ' + '@app.route("/buildRelatednessNet/bypublisher", methods=[\'POST\'])')
        log.logger.debug('route : ' + '@app.route("/buildRelatednessNet/getrelations"')
        if "dbname" in request.args:
            if "collection" in request.args:

                dbname = request.args.get('dbname')
                collection = request.args.get('collection')




        #logger.debug('request : ' + request.query_string)
        #if request.get_json(force=True):
        #    global responsee
        #    responsee = request.get_json(force=True)
        #    print responsee

                publishersCollection = mongo.connect("localhost",27017,dbname,collection)
                publisherrelationsecursor = mongo.getfromMongo(publishersCollection, "")
                #print distinctpublisherecursor.next
                for relation in  publisherrelationsecursor:
                    print relation
                    if "publisherA" in relation:
                        if "publisherB" in relation:
                            if "relationStrength" in relation:
                                if relation["publisherA"] and relation["publisherB"] and relation["relationStrength"]:
                                    rel = (relation["publisherA"],relation["publisherB"],relation["relationStrength"])
                                    print rel
                                    response_data['result'].append(rel)


                response_data['success'] = True
                # adding service messages
                response_data['messages'] = messages
                json_response = json.dumps(response_data)




    except Exception, e:
        log.logger.error(e)
        json_response=error_resonse(str(traceback.print_exc()))
        traceback.print_exc()


    return json_response


#dbname and collection (analyzed datasets) name expected here
#http://localhost:5001/buildRelatednessNet/getnodes?dbname=rtpa&collection=rtpaG
@app.route("/buildRelatednessNet/getnodes")#, methods=['POST'])
def getnodes():
    response_data = {}
    # response_results=json.dumps(response_results_list)
    response_data['result'] = []


    try:
        #logger.debug('route : ' + '@app.route("/buildRelatednessNet/bypublisher", methods=[\'POST\'])')
        log.logger.debug('route : ' + '@app.route("/buildRelatednessNet/getnodes"')
        if "dbname" in request.args:
            if "collection" in request.args:

                dbname = request.args.get('dbname')
                collection = request.args.get('collection')




        #logger.debug('request : ' + request.query_string)
        #if request.get_json(force=True):
        #    global responsee
        #    responsee = request.get_json(force=True)
        #    print responsee

                publishersCollection = mongo.connect("localhost",27017,dbname,collection)
                distinctpublisherecursor = mongo.getdistinctfromMongo(publishersCollection, "publisherId","")
                #print distinctpublisherecursor.next
                #for relation in  distinctpublisherecursor:

                log.logger.info("distinctpublisherecursor")
                log.logger.info(distinctpublisherecursor)

                if distinctpublisherecursor:
                    for node in distinctpublisherecursor:
                        print node
                        response_data['result'].append(node)


                response_data['success'] = True
                # adding service messages
                response_data['messages'] = messages
                json_response = json.dumps(response_data)


    except Exception, e:
        log.logger.error(e)
        json_response=error_resonse(str(traceback.print_exc()))
        traceback.print_exc()


    return json_response

@app.route('/<string:page_name>')
def ok(page_name):
    print page_name
    #return render_template('%s.html' % page_name)
    return app.send_static_file('%s.html' % page_name)


@app.route('/js/<path:path>')
def send_js(path):
    return app.send_static_file('js/%s' % path)



@app.route('/files/<path:path>')
def send_file(path):
    #return app.send_static_file('sigmajs/%s' % path)
    #f#ile='static/%s' % path
    return send_from_directory('static', path)



@app.route('/test')
def why():
    #print page_name
    #return render_template('%s.html' % page_name)
    #return app.send_static_file("data3.json")
    #return "<a href=%s>file</a>" % url_for('static', filename='data3.json')
    #return send_file('static/data3.json', content_type='application/json')
    return send_from_directory('static', 'data3.json')

def error_resonse(excp):

    #print excp.message
    #print excp.args
    #print excp
    json_response = json.dumps({'success': False, 'error': excp, 'messages': messages })
    log.logger.debug('response : ' + json.dumps(json_response))
    # return JsonResponse({'success': False, "message": str(e)})


@app.route('/buildRelatednessNet/getdatareadyforsigmajs/pub/')
def getdatareadyforsigmajs_pub():

    json_readyforsigma = {}
    nodes =[]
    edges = []

    x=0
    y=0
    #creating nodes
    node = {}
    publishers = json.loads(getnodes_publisher(db,coll,relstren))#rtpaG #dublinked_3

    log.logger.info("publishers")
    log.logger.info(publishers)


    from random import randint
    #print()
    for nodee in publishers["result"]:
        node.clear()
        print nodee
        node["id"] = str(nodee)
        node["label"]= str(nodee)
        node["size"]=3

        x += 1
        y += 1
        #node["x"] = randint(0, len(publishers["result"]))+5
        #node["y"] = randint(0, len(publishers["result"]))+5
        node["x"] = randint(0, len(publishers["result"]))
        node["y"] = randint(0, len(publishers["result"]))
        #node["x"] = randint(0, 5)
        #node["y"] = randint(0, 5)
        import copy
        nodes.append(copy.deepcopy(node))


        #from time import sleep
        #sleep(0.05)



    # creating edges
    edge = {}
    relations = json.loads(getedges_publisher(db,coll,relstren))#rtpaG_PubllishersNetwork7 # dublinked_3_PubllishersNetwork
    #print edges
    for edgee in relations["result"]:
        edge.clear()
        edge["id"] = str(edgee[0]) + str(edgee[1])
        edge["source"]= str(edgee[0])
        edge["target"]= str(edgee[1])
        edge["size"] = int(edgee[2])

        #edges.append(edge.copy())
        import copy
        edges.append(copy.deepcopy(edge))
        #from time import sleep
        #sleep(0.05)





    json_readyforsigma['nodes'] = nodes
    json_readyforsigma ['edges'] = edges
    #print json_readyforsigma
    print json.dumps(json_readyforsigma)
    return json.dumps(json_readyforsigma)



def getnodes_publisher(dbname, collection, minimumStrenghth):

    response_data = {}
    # response_results=json.dumps(response_results_list)
    response_data['result'] = []

    #dbname = request.args.get('dbname')
    #collection = request.args.get('collection')

    #dbname = 'rtpa'
    #collection = 'rtpaG'

    # logger.debug('request : ' + request.query_string)
    # if request.get_json(force=True):
    #    global responsee
    #    responsee = request.get_json(force=True)
    #    print responsee



    # connect
    publishersCollection = mongo.connect("localhost", 27017, dbname, collection)

    # initiate query structure
    pipeline = []
    query = {}
    project = {}
    commonToBoth = {}
    setIntersection = []
    id = 0

    # fill query structure
    setIntersection.append(publishersCollection.distinct("publisherA"))
    setIntersection.append(publishersCollection.distinct("publisherB"))
    commonToBoth["$setIntersection"] = setIntersection
    project["commonToBoth"] = commonToBoth
    project["_id"] = id
    query["$project"] = project

    #alternate way of filling queries
    q2 = {"$project": {"commonToBoth": {
        "$setIntersection": [publishersCollection.distinct("publisherA"), publishersCollection.distinct("publisherB")]},
                       "_id": 0}}

    q3 = {'$project': {'UniqueUnion':
                           {'$setUnion':
                                [list(publishersCollection.aggregate([{"$match": {"relationStrength": {"$gt": minimumStrenghth}}},
                                                                  {"$project": {"publisher": "$publisherA", "_id": 0}}
                                                                  ]))
                                    , list(publishersCollection.aggregate([
                                    {"$match": {"relationStrength": {"$gt": minimumStrenghth}}},
                                    {"$project": {"publisher": "$publisherB", "_id": 0}}
                                ]))
                                 ]
                            }
        , "_id": 0
                       }
          }
    pipeline.append(q3)

    distinctpublisherecursor = mongo.aggregateMongo(publishersCollection, pipeline)

    #print distinctpublisherecursor.next()

        # print distinctpublisherecursor.next
    # for relation in  distinctpublisherecursor:
    #print distinctpublisherecursor
    if distinctpublisherecursor:

        for cursor in distinctpublisherecursor:
            uniquePublishers = cursor["UniqueUnion"]
        for node in uniquePublishers:
            print node["publisher"]
            response_data['result'].append(node["publisher"])






    response_data['success'] = True
    # adding service messages
    response_data['messages'] = messages
    json_response = json.dumps(response_data)
    print json_response
    return json_response

def getedges_publisher(dbname,collection, minimumStrenghth):
    response_data = {}
    # response_results=json.dumps(response_results_list)
    response_data['result'] = []

    try:


            #dbname = request.args.get('dbname')
            #collection = request.args.get('collection')
            #dbname = "rtpa"
            #collection = "rtpaG_PubllishersNetwork7"
            #collection = "rtpaG_PubllishersNetwork"

            # logger.debug('request : ' + request.query_string)
            # if request.get_json(force=True):
            #    global responsee
            #    responsee = request.get_json(force=True)
            #    print responsee

            #relation strenghth filtring
            query = {}
            query['relationStrength'] = {}
            inner_query = {}
            inner_query['$gt'] = minimumStrenghth
            query['relationStrength'] = inner_query

            print query


            publishersCollection = mongo.connect("localhost", 27017, dbname, collection)
            publisherrelationsecursor = mongo.getfromMongo(publishersCollection, query)
            # print distinctpublisherecursor.next
            print ">>>>>>>>>"
            print publisherrelationsecursor.next
            for relation in publisherrelationsecursor:
               # print relation
                if "publisherA" in relation:
                    if "publisherB" in relation:
                        if "relationStrength" in relation:
                            if relation["publisherA"] and relation["publisherB"] and relation["relationStrength"]:
                                rel = (relation["publisherA"], relation["publisherB"], relation["relationStrength"])
                                #print rel
                                response_data['result'].append(rel)

            response_data['success'] = True
            # adding service messages
            response_data['messages'] = messages
            json_response = json.dumps(response_data)




    except Exception, e:
        log.logger.error(e)
        json_response = error_resonse(str(traceback.print_exc()))
        traceback.print_exc()

    return json_response



@app.route('/buildRelatednessNet/getdatareadyforsigmajs/ds/')
def getdatareadyforsigmajs_ds():

    json_readyforsigma = {}
    nodes =[]
    edges = []

    x=0
    y=0
    #creating nodes
    node = {}
    publishers = json.loads(getnodes_Dataset())
    print publishers
    from random import randint
    #print()
    for nodee in publishers["result"]:
        node.clear()
        print nodee
        node["id"] = str(nodee)
        node["label"]= str(nodee)
        node["size"]=3

        x += 1
        y += 1
        #node["x"] = randint(0, len(publishers["result"]))+5
        #node["y"] = randint(0, len(publishers["result"]))+5
        node["x"] = randint(0, len(publishers["result"]))
        node["y"] = randint(0, len(publishers["result"]))
        nodes.append(node.copy())

        #from time import sleep
        #sleep(0.05)



    # creating edges
    edge = {}
    relations = json.loads(getedges_Dataset())
    #print edges
    for edgee in relations["result"]:
        edge.clear()
        edge["id"] = str(edgee[0]) + str(edgee[1])
        edge["source"]= str(edgee[0])
        edge["target"]= str(edgee[1])
        edge["size"] = int(edgee[2])

        edges.append(edge.copy())

        #from time import sleep
        #sleep(0.05)





    json_readyforsigma['nodes'] = nodes
    json_readyforsigma ['edges'] = edges
    #print json_readyforsigma
    print json.dumps(json_readyforsigma)
    return json.dumps(json_readyforsigma)

def getnodes_Dataset():

    response_data = {}
    # response_results=json.dumps(response_results_list)
    response_data['result'] = []

    #dbname = request.args.get('dbname')
    #collection = request.args.get('collection')

    dbname = 'rtpa'
    #collection = 'rtpaG'
    collection = 'rtpa_dublin_tet_4'
    # logger.debug('request : ' + request.query_string)
    # if request.get_json(force=True):
    #    global responsee
    #    responsee = request.get_json(force=True)
    #    print responsee

    publishersCollection = mongo.connect("localhost", 27017, dbname, collection)
    distinctpublisherecursor = mongo.getdistinctfromMongo(publishersCollection, "DatasetId", "")
    # print distinctpublisherecursor.next
    # for relation in  distinctpublisherecursor:
    #print distinctpublisherecursor
    if distinctpublisherecursor:
        for node in distinctpublisherecursor:
           # print node
            response_data['result'].append(node)

    response_data['success'] = True
    # adding service messages
    response_data['messages'] = messages
    json_response = json.dumps(response_data)

    return json_response

def getedges_Dataset():
    response_data = {}
    # response_results=json.dumps(response_results_list)
    response_data['result'] = []

    try:


            #dbname = request.args.get('dbname')
            #collection = request.args.get('collection')
            dbname = "rtpa"
            #collection = "rtpaG_PubllishersNetwork7"
            #collection = "rtpaG_PubllishersNetwork"
            collection = 'rtpa_dublin_tet_4_DatasetsNetwork55'

            # logger.debug('request : ' + request.query_string)
            # if request.get_json(force=True):
            #    global responsee
            #    responsee = request.get_json(force=True)
            #    print responsee

            #relation strenghth filtring
            query = {}
            query['relationStrength'] = {}
            inner_query = {}
            inner_query['$gt'] = 0
            query['relationStrength'] = inner_query

            print query


            publishersCollection = mongo.connect("localhost", 27017, dbname, collection)
            publisherrelationsecursor = mongo.getfromMongo(publishersCollection, query)
            # print distinctpublisherecursor.next
            print ">>>>>>>>>"
            print publisherrelationsecursor.next
            for relation in publisherrelationsecursor:
               # print relation
                if "datasetA" in relation:
                    if "datasetB" in relation:
                        if "relationStrength" in relation:
                            if relation["datasetA"] and relation["datasetB"] and relation["relationStrength"]:
                                rel = (relation["datasetA"], relation["datasetB"], relation["relationStrength"])
                                #print rel
                                response_data['result'].append(rel)

            response_data['success'] = True
            # adding service messages
            response_data['messages'] = messages
            json_response = json.dumps(response_data)




    except Exception, e:
        log.logger.error(e)
        json_response = error_resonse(str(traceback.print_exc()))
        traceback.print_exc()

    return json_response

if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=int("5001")
    )
