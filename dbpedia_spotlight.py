#import spotlight
import json
import copy
from skelton import DataSet, Relation
from loggiingg import logee



class Dbpedia:

    # error and algorithem decisions tracing to be added to response
    global message
    messages = []

    #spotlight_server = "http://vmogi01.deri.ie:8001/rest/annotate"
    global spotlight_server
    spotlight_server = "http://localhost:2222/rest/annotate"

    #initiate logger object
    global log
    log = logee("collaboration.log","rtpa-network-builder-dbpedia_spotlight_py-Dbpedia")

    #global text
    #text= "President Obama on Monday will call for a new minimum tax rate for individuals making more than $1 million a year to ensure that they pay at least the same percentage of their earnings as other taxpayers, according to administration officials."

    """
    annotations = spotlight.annotate('http://localhost/rest/annotate',
                                      "your text",
                                     confidence=0.4, support=20)


    custome_filter = {

        'policy': "whitelist",

        'types': "DBpedia:Organisation,DBpedia:Person,DBpedia:Place,Freebase:/location,Freebase:/organization,Freebase:/people,Schema:Organization,Schema:Person,Schema:Place",

        'coreferenceResolution': False
    }
    """
    def get_dbpedia_NER(slef, dataset):
        dbpedia_rowNER = {}
        dbpedia_NER = []
        textwithoutNationalStaff_list = []
        #textwithoutNationalStaff_list = []
        textwithoutNationalStaff = ""
        #text=""
        final_uniqe_dic = {}
        final_uniqe_list_of_dic = []
        annotations_list = []
        try:
            text = copy.deepcopy(dataset.constructTextualRepresentation())
            for i in text.split():
                    if i not in ["Ireland", "ireland", "IRELAND", "Ireland.", "ireland.", "IRELAND.","Ireland,", "ireland,", "IRELAND,"]:
                        "remove ireland lower case (AND)"
                        if i in ["AND"]:
                            textwithoutNationalStaff_list.append(i.lower())
                        else:
                            textwithoutNationalStaff_list.append(i)

            #getting unique only (wasn't a good idea)
            #textwithoutNationalStaff_list = list(set(textwithoutNationalStaff_list))

            for str in textwithoutNationalStaff_list:
                textwithoutNationalStaff += str +" "

                textwithoutNationalStaff= textwithoutNationalStaff.replace("-"," ")

            # logging to service.log and to screen
            log.logger.info( "text fed to spotlight")
            log.logger.info( textwithoutNationalStaff)
            del textwithoutNationalStaff_list [:]


            try:

                annotations_list = spotlight.annotate(spotlight_server, textwithoutNationalStaff, confidence=0.4,
                                                      support=20)#filters=custome_filter)
                # logging to service.log and to screen
                log.logger.info("spotlight response")
                log.logger.info(annotations_list)

                for entity_dic in annotations_list:
                    if "surfaceForm" in entity_dic:
                        dbpedia_rowNER['entity'] = copy.deepcopy(entity_dic["surfaceForm"])
                    if "types" in entity_dic:
                        types_list = entity_dic["types"].split(',')
                        if types_list[0]:
                            dbpedia_rowNER['type'] = copy.deepcopy(types_list[0])
                            del types_list [:]
                        else:
                            dbpedia_rowNER['type'] = "DBpedia:Thing"
                            """we need to check for yago type here!!!"""
                            del types_list[:]
                        # import copy

                    dbpedia_NER.append(copy.deepcopy(dbpedia_rowNER))
                    dbpedia_rowNER.clear

                    # trying to enhance the performance of NER results nGram staff
                    # for NE in rowNER:
                    #   print
                    # NER=rowNER


                """uniques only"""

                temp_list = []

                for d in dbpedia_NER:
                    temp_str = copy.deepcopy(d["entity"])
                    temp_list.append(temp_str.lower())

                list_unique_entities = list(set(temp_list))

                # logging to service.log and to screen
                log.logger.info("spotlight response uniques")
                log.logger.info(list_unique_entities)

                """note: if an entity have multible types the first type only will be taken into account other types are disregarded"""

                for v in list_unique_entities:
                    for d in dbpedia_NER:
                        if d["entity"].lower() == v:
                            final_uniqe_dic["type"] = copy.deepcopy(d["type"])
                            final_uniqe_dic["entity"] = copy.deepcopy(d["entity"])
                            final_uniqe_list_of_dic.append(copy.deepcopy(final_uniqe_dic))
                            final_uniqe_dic.clear()
                            break


                # logging to service.log and to screen
                log.logger.info("final_uniqe_list_of_dic")
                log.logger.info(final_uniqe_list_of_dic)

                dataset.DBpedia_Entities = copy.deepcopy(final_uniqe_list_of_dic)
                # logging to service.log and to screen
                log.logger.info("dataset.DBpedia_Entities")
                log.logger.info(dataset.DBpedia_Entities)
                del dbpedia_NER[:]
                del annotations_list [:]
                textwithoutNationalStaff = ""
                del  final_uniqe_list_of_dic [:]



            except Exception , e:
                # logging to service.log and to screen
                log.logger.error("Exception")
                log.logger.error(e)
                #annotations_list.append(e)
                #print annotations_list
                del dbpedia_NER[:]
                del annotations_list[:]
                textwithoutNationalStaff = ""
                del final_uniqe_list_of_dic[:]




        except Exception, e:
            # logging to service.log and to screen
            log.logger.error("Exception")
            log.logger.error(e)
            #print e
            textwithoutNationalStaff=""
            text=""




    #mohade@mohade-ThinkPad-X1-Carbon-2nd:~/workspace/rtpa-dbpedia-spotlight$ java -Xmx5G -Xms5G -jar dbpedia-spotlight-latest.jar en http://localhost:2222/rest


"""
:param policy:
 -        The policy to be used.
 -    :type policy: string
 -
 -    :param types:
 -        The types of resources that will be included in the response,
 -        in accordance with the policy (whitelist or blacklist).
 -    :type types: string
 +    :param filters:
 +        Additional parameters that collectively define a filter function.
 +
 +        For example:
 +        'policy'                (string)
 +                                The policy to be used:
 +                                'whitelist' or 'blacklist';
 +        'types'                 (string)
 +                                Comma-separated list of types,
 +                                i.e. 'DBpedia:Agent,Schema:Organization';
 +        'sparql'                (string)
 +                                Select only entities that (don't)
 +                                match with the SPARQL query result;
 +        'coreferenceResolution' (boolean)
 +                                Annotate coreferences: true / false.
 +                                Set to false to use types (statistical only).
 +
 +    :type filters: string



types: "DBpedia:Organisation,DBpedia:Person,DBpedia:Place,Freebase:/location,Freebase:/organization,Freebase:/people,Schema:Organization,Schema:Person,Schema:Place"

"""