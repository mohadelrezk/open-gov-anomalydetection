

from dataaquestion import DataAquestion
from stanfordNER import StanfordNER
from collaborationanlysis import CollaborationAnalysis
from textAnalysis import TextAnalysis
from networkAnalysisForPoster import NetworkAnalysis

import sys

import gc

gc.enable()

dataPortal=""
mongoDomain=""
mongoPort=0 # must be integer
dbName=""
dataSetsIdsCollection=""
dataSetsCollection=""
startFrom = 0
NER = False
#NERrefine = False


print 'Number of arguments:', len(sys.argv), 'arguments.'
print 'Argument List:', str(sys.argv)

try:

    if sys.argv[1]:
        dataPortal=sys.argv[1]

    if sys.argv[2]:
        mongoDomain=sys.argv[2]

    if sys.argv[3]:
        mongoPort=int(sys.argv[3])

    if sys.argv[4]:
        dbName=sys.argv[4]

    if sys.argv[5]:
        dataSetsIdsCollection=sys.argv[5]

    if sys.argv[6]:
        dataSetsCollection=sys.argv[6]

    if sys.argv[7]:
        startFrom=int(sys.argv[7])

    if sys.argv[8]:
        if sys.argv[8] == 'True' or sys.argv[8] == 'true':
            NER = True
            print 'ner is true'
        else:
            NER = False
            print 'ner is false'

except BaseException as exce:
        print exce




global dataAquestion
global stanfordNER
global collaborationanlaysis
global textanalysis
global networkanalysis


dataAquestion = DataAquestion(dataPortal, mongoDomain, mongoPort, dbName, dataSetsIdsCollection, dataSetsCollection)
stanfordNER = StanfordNER()
collaborationanlaysis = CollaborationAnalysis(dataPortal, mongoDomain, mongoPort, dbName, dataSetsIdsCollection, dataSetsCollection)
textanalysis = TextAnalysis(dataPortal, mongoDomain, mongoPort, dbName, dataSetsIdsCollection, dataSetsCollection)
comparsionfield = "Stanford_NERrefined"#"DBpedia_Entities"#"Stanford_NERrefined"#"Stanford_Entities"
outputFolder = 'poster/poster_stanford-refined/'
networkanalysis = NetworkAnalysis(dataPortal, mongoDomain, mongoPort, dbName, dataSetsIdsCollection, dataSetsCollection,outputFolder, comparsionfield)

if len(sys.argv) >= 7:

    """ pipeline logic"""

    """ Start Harvesting and Entity detection"""
    #dataAquestion.constructIdsList()
    #dataAquestion.harvestDatasets(startFrom)#2612
    #dataAquestion.harvestDatasets(4785, NER)#2612

    """ TextAnalysis"""
    #textanalysis.analyze(0)

    """incase stanford NER is used we might run the 3Gram refinments on the whole dataset"""
    #stanfordNER.NERrefining(mongoDomain, mongoPort, dbName, dataSetsCollection)

    """ Building collaboration network for datasets"""
    #collaborationanlaysis.buildDataSetNetwork(comparsionfield)


    """ Building collaboration network for dataset publishers"""
    #collaborationanlaysis.buildPublisherNetwork(comparsionfield)

    """ Run Visulization Service"""
    # python rtpa-publisher-network-builder.py

    """ Run analysis service (for publications)"""
    networkanalysis.run()

else:
    print "args missing"


