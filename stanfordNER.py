
from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
import copy

from skelton import DataSet, Relation
from mongo import Mongo
from pymongo import CursorType
from loggiingg import logee


class StanfordNER:


    # initiate logger object
    global log
    log = logee("collaboration.log", "rtpa-network-builder-stanfordNER_py-StanfordNER")



    def NER(self, dataset):
        textwithoutNationalStaff_list = []
        textwithoutNationalStaff = ""
        rowNER={}
        NER = []
        try:
            text = copy.deepcopy(dataset.constructTextualRepresentation())
            # NER logic
            #NLTK
            st = StanfordNERTagger('/home/mohade/workspace/rtpa-network-builder/stanford-ner/classifiers/english.all.3class.distsim.crf.ser.gz',
                                   '/home/mohade/workspace/rtpa-network-builder/stanford-ner/stanford-ner.jar',
                                   encoding='utf-8')

            #text = 'While in France, Christine Lagarde discussed short-term stimulus efforts in a recent interview with the Wall Street Journal.'


            #replace dashes
            text = text.replace("-", " ")

            #remove stop words
            from nltk.corpus import stopwords
            stop = set(stopwords.words('english'))
            textwithoutstopwords = ""
            for i in text.split():
                    if i not in stop:
                        #execludinng national data
                        if i not in ["Ireland", "ireland", "IRELAND", "Ireland.", "ireland.", "IRELAND.", "Ireland,",
                                     "ireland,", "IRELAND,"]:
                            textwithoutNationalStaff_list.append(i)

            for str in textwithoutNationalStaff_list:
                textwithoutNationalStaff += str +" "

            tokenized_text = word_tokenize(textwithoutNationalStaff)
            classified_text = st.tag(tokenized_text)
            del textwithoutNationalStaff_list[:]

            print(classified_text)

            """
            3 class:	Location, Person, Organization
            4 class:	Location, Person, Organization, Misc
            7 class:	Location, Person, Organization, Money, Percent, Date, Time

            """
            for tuble in classified_text:
                if tuble[1] == 'LOCATION' or  tuble[1] == 'PERSON' or  tuble[1] == 'ORGANIZATION':
                    rowNER['entity']= (tuble[0])
                    rowNER['type'] = (tuble[1])
                    #import copy
                    NER.append(copy.deepcopy(rowNER))
                    rowNER.clear

            #trying to enhance the performance of NER results nGram staff
            #for NE in rowNER:
             #   print
            #NER=rowNER
            dataset.Stanford_Entities = copy.deepcopy(NER)
            print NER
            del NER[:]
            textwithoutNationalStaff = ""

        except Exception,e:

            NER.append(e)
            dataset.Stanford_Entities = NER
            print e
    def comment(self):
        """
                stoping it to get relevant 3ngrams entities

                            #uniques only

                            temp_list = []

                            for d in NER:
                                temp_str = copy.deepcopy(d["entity"])
                                temp_list.append(temp_str.lower())

                            list_unique_entities = list(set(temp_list))

                            # print list_of_dic
                            # print temp_list
                            # print list_unique

                          #note: if an entity have multible types the first type only will be taken into account other types are disregarded
                            final_uniqe_dic = {}
                            final_uniqe_list_of_dic = []
                            for v in list_unique_entities:
                                for d in NER:
                                    if d["entity"].lower() == v:
                                        final_uniqe_dic["type"] = copy.deepcopy(d["type"])
                                        final_uniqe_dic["entity"] = copy.deepcopy(d["entity"])
                                        final_uniqe_list_of_dic.append(copy.deepcopy(final_uniqe_dic))
                                        final_uniqe_dic.clear()
                                        break

                            print final_uniqe_list_of_dic

                            dataset.Stanford_Entities = copy.deepcopy(final_uniqe_list_of_dic)
                            print NER
                            del NER[:]
                            textwithoutNationalStaff = ""
                            del final_uniqe_list_of_dic[:]
        """

    def NERrefining(self, mongodomain, mongoport, dbname, datasetsCollection):
        mongo = Mongo()

        NERrefined = []
        dataset={}
        counter=0
        datasetSetscursor= CursorType

        final_uniqe_dic = {}
        final_uniqe_list_of_dic = []
        annotations_list = []
        refinedCollectionName = ""
        datesetscollectionToGetFrom = ""
        try:
            # connect mongo to get entities
            datesetscollectionToGetFrom = datasetsCollection + "_AfterTextAnalysis"
            datesetscollection = mongo.connect(mongodomain, mongoport, dbname, datesetscollectionToGetFrom)
            datasetSetscursor = mongo.getfromMongo(datesetscollection, '')
            log.logger.info('def NERrefining(self): mongo fine!')

            for object in datasetSetscursor:
                log.logger.info("counter")
                log.logger.info(counter)
                # print ob
                dataset = object
                if len(object['Stanford_Entities']) > 0:

                    dataset= object
                    Entities = object['Stanford_Entities']
                    print Entities
                    # cleaning ner refined
                    del NERrefined[:]

                    for x in range (0 , len(Entities)):
                        entity={}
                        if Entities[x]['type'] == 'ORGANIZATION':
                            entity['type'] = "ORGANIZATION"
                            entity['entity'] = Entities[x]['entity']
                            #print entity
                            if (x+1) < len(Entities):
                                if Entities[x+1]['type'] == 'ORGANIZATION':
                                    #avoiding redundunt names
                                    if entity['entity'] != str(Entities[x+1]['entity']):
                                        entity['entity'] += ' '+ str(Entities[x+1]['entity'])
                                    #print entity
                                    if (x + 2) < len(Entities):
                                        if Entities[x + 2]['type'] == 'ORGANIZATION':
                                            if entity['entity'] != str(Entities[x + 2]['entity']):
                                                entity['entity'] += ' ' + Entities[x + 2]['entity']

                        elif Entities[x]['type'] == 'PERSON':
                            entity['type'] = "PERSON"
                            entity['entity'] = Entities[x]['entity']
                            if (x + 1) < len(Entities):
                                if Entities[x + 1]['type'] == 'PERSON':
                                    # avoiding redundunt names
                                    if entity['entity'] != str(Entities[x + 1]['entity']):
                                        entity['entity'] += ' ' + Entities[x + 1]['entity']
                                    if (x + 2) < len(Entities):
                                        if Entities[x + 2]['type'] == 'PERSON':
                                            # avoiding redundunt names
                                            if entity['entity'] != str(Entities[x + 2]['entity']):
                                                entity['entity'] += ' ' + Entities[x + 2]['entity']

                        elif Entities[x]['type'] == 'LOCATION':
                            entity['type'] = "LOCATION"
                            entity['entity'] = Entities[x]['entity']
                            if (x + 1) < len(Entities):
                                if Entities[x + 1]['type'] == 'LOCATION':
                                    # avoiding redundunt names
                                    if entity['entity'] != str(Entities[x + 1]['entity']):
                                        entity['entity'] += ' ' + Entities[x + 1]['entity']
                                    if (x + 2) < len(Entities):
                                        if Entities[x + 2]['type'] == 'LOCATION':
                                            # avoiding redundunt names
                                            if entity['entity'] != str(Entities[x + 2]['entity']):
                                                entity['entity'] += ' ' + Entities[x + 2]['entity']

                        print entity['entity']
                        import copy
                        NERrefined.append(copy.deepcopy(entity))
                        entity.clear
                        #dataset['NERrefined'] = []



                    if len (NERrefined) > 1:
                        try:
                            """uniques only"""

                            temp_list = []

                            for d in NERrefined:
                                temp_str = copy.deepcopy(d["entity"])
                                temp_list.append(temp_str.lower())

                            list_unique_entities = list(set(temp_list))

                            # logging to service.log and to screen
                            log.logger.info("StanfordNER refined full")
                            log.logger.info(NERrefined)

                            # logging to service.log and to screen
                            log.logger.info("StanfordNER refined uniques")
                            log.logger.info(list_unique_entities)

                            """note: if an entity have multible types the first type only will be taken into account other types are disregarded"""

                            for v in list_unique_entities:
                                for d in NERrefined:
                                    if d["entity"].lower() == v:
                                        final_uniqe_dic["type"] = copy.deepcopy(d["type"])
                                        final_uniqe_dic["entity"] = copy.deepcopy(d["entity"])
                                        final_uniqe_list_of_dic.append(copy.deepcopy(final_uniqe_dic))
                                        final_uniqe_dic.clear()
                                        break

                            # logging to service.log and to screen
                            log.logger.info("final_uniqe_list_of_dic/dbpedia refined")
                            log.logger.info(final_uniqe_list_of_dic)

                            #dbase entry
                            dataset['Stanford_NERrefined'] = copy.deepcopy(final_uniqe_list_of_dic)
                            refinedCollectionName = datasetsCollection + "_After3gramRefinment"
                            datesetscollectionToAppendin = mongo.connect(mongodomain, mongoport, dbname, refinedCollectionName)
                            mongo.appendDataSetToMongo(datesetscollectionToAppendin, dataset)
                            counter += 1
                            log.logger.error(counter)
                            log.logger.error("ok!")

                            # logging to service.log and to screen
                            #log.logger.info("dataset['Stanford_NERrefined']")
                            #log.logger.info(dataset.DBpedia_Entities)
                            del NERrefined[:]
                            del annotations_list[:]
                            textwithoutNationalStaff = ""
                            del final_uniqe_list_of_dic[:]

                        except Exception, e:
                            # logging to service.log and to screen
                            log.logger.error("Exception")
                            log.logger.error(e)
                            # annotations_list.append(e)
                            # print annotations_list

                            # dbase entry embty
                            dataset['Stanford_NERrefined'] = []
                            # print 'ok'
                            # print json.dumps(dataset)
                            refinedCollectionName = datasetsCollection + "_After3gramRefinment"
                            datesetscollectionToAppendin = mongo.connect(mongodomain, mongoport, dbname,
                                                                         refinedCollectionName)
                            mongo.appendDataSetToMongo(datesetscollectionToAppendin, dataset)

                            counter += 1
                            log.logger.error(counter)
                            log.logger.error("embty!")

                            del NERrefined[:]
                            del annotations_list[:]
                            textwithoutNationalStaff = ""
                            del final_uniqe_list_of_dic[:]
                    else:
                        # dbase entry
                        dataset['Stanford_NERrefined'] = []
                        refinedCollectionName = datasetsCollection + "_After3gramRefinment"
                        datesetscollectionToAppendin = mongo.connect(mongodomain, mongoport, dbname,
                                                                     refinedCollectionName)
                        mongo.appendDataSetToMongo(datesetscollectionToAppendin, dataset)
                        counter += 1
                        log.logger.error(counter)
                        log.logger.error("empty refined list!")


                else:
                    # dbase entry
                    dataset['Stanford_NERrefined'] = []
                    refinedCollectionName = datasetsCollection + "_After3gramRefinment"
                    datesetscollectionToAppendin = mongo.connect(mongodomain, mongoport, dbname, refinedCollectionName)
                    mongo.appendDataSetToMongo(datesetscollectionToAppendin, dataset)
                    counter += 1
                    log.logger.error(counter)
                    log.logger.error("empty source Stanford!")



            #avoiding curser id not found by setting no time limit to true which require manual cursor closing
            datasetSetscursor.close()

        except Exception,e:
            log.logger.error ('def NERrefining(self):')
            # logging to service.log and to screen
            log.logger.error("Exception")
            log.logger.error(e)

            # dbase entry embty
            dataset['Stanford_NERrefined'] = []
            # print 'ok'
            # print json.dumps(dataset)
            refinedCollectionName = datasetsCollection + "_After3gramRefinment"
            datesetscollectionToAppendin = mongo.connect(mongodomain, mongoport, dbname, refinedCollectionName)
            mongo.appendDataSetToMongo(datesetscollectionToAppendin, dataset)

            counter += 1
            log.logger.error(counter)
            log.logger.error("embty!")

            log.logger.error (counter)
            datasetSetscursor.close()