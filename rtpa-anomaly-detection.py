# weserver
from flask import Flask, request
from flask_cors import CORS
# anaomaly LOF library
from lof import LOF
# handling json data
import json
# categoral factorization and maths functions
import pandas as pd
import numpy as np
# logging
import logging.handlers
# forcing ram cleaning
import gc
# enable full error tracing in responses
import traceback

# error and algorithem decisions tracing to be added to response
global message
messages = []

# creating flask server object
app = Flask(__name__)
# enabling cross origin data communication
CORS(app)

# logging configuration

LOG_FILENAME = 'service.log'
# create logger
logger = logging.getLogger("rtpa-anomaly-detection")
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
consolehandler = logging.StreamHandler()
# Add the log message handler to the logger
logFilehandler = logging.handlers.RotatingFileHandler(
    LOG_FILENAME, maxBytes=20000000, backupCount=5)
consolehandler.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
consolehandler.setFormatter(formatter)
logFilehandler.setFormatter(formatter)
# add ch to logger
logger.addHandler(consolehandler)
logger.addHandler(logFilehandler)

# enabling garabage collector
gc.enable()

"""check service status"""


@app.route('/detectAnomalies/lof')
def checkiflive():
    return "I am live !"


"""resource quality detection service"""


@app.route("/detectAnomalies/resource_quality", methods=['POST'])
def quality():
    # initializing global variables
    # holding dataset records (rows)
    global records
    # final service response
    global responsee
    # making sure global message variable is clean
    del messages[:]
    # try catch statemnt for error handling
    try:
        # logging information
        logger.debug('route : ' + '@app.route("detectAnomalies/resource_quality", methods=[\'POST\'])')
        logger.debug('request : ' + request.query_string)
        # checking POST request data embty or filled
        if request.get_json(force=True):
            # if data is fine put it in global responsee and global records
            responsee = request.get_json(force=True)
            print responsee
            records = responsee["result"]["records"]
            # create cash file name for the data to compare with exsiting files before caculate new
            file_name = creating_file_name("resource_quality")

            # if file name not error.json which means that the creating file name function went fine
            if file_name != "error.json":
                # check previous responses in output directory
                file_found = search_output_dir_for_previous_responses(file_name, "output/")

                # if file found load its content and put it in response
                if file_found:
                    with open(file_found) as previous_response_file:
                        json_response = json.load(previous_response_file)

                # if not found make new calculations
                else:
                    json_response = quality_calculation()

            # if file name is error.json make new calculation
            else:
                json_response = quality_calculation()

        # if request data is malfunctioning
        else:
            # logging to service.log and to screen
            logger.error("request.get_json(force=True)> looks request data is embty?")
            print "request.get_json(force=True)> looks request data is embty?"
            # adding error to response message
            json_response = error_resonse("request.get_json(force=True)> looks request data is embty?")


    # incase comiler catched an error
    except Exception, e:
        # logging error
        logger.error(e)
        # adding error to response message
        json_response = error_resonse(str(traceback.print_exc()))
        # printing full error trace
        traceback.print_exc()

    # returing service response to requesting client
    return json_response


"""anomaly detection sevice"""


@app.route("/detectAnomalies/lof", methods=['POST'])
def lof():
    # initializing global variables
    # holding dataset records (rows)
    global records
    # final service response
    global responsee
    # making sure global message variable is clean
    del messages[:]
    # try catch statemnt for error handling

    try:
        # logging information
        logger.debug('route : ' + '@app.route("/detectAnomalies/lof", methods=[\'POST\'])')
        logger.debug('request : ' + request.query_string)
        # checking POST request data embty or filled
        if request.get_json(force=True):
            # if data is fine put it in global responsee and global records
            responsee = request.get_json(force=True)
            print responsee
            records = responsee["result"]["records"]
            """
            #temporary using 500 rcords
            max = 500
            if len(responsee['result']['records']) <500:
                max = len(responsee['result']['records'])

            for i in xrange(0, max):
                records.append(responsee['result']['records'][i])
            messages.append('temporary using max 500 records')
            """

            # create cash file name for the data to compare with exsiting files before caculate new
            file_name = creating_file_name('lof')

            # if file name not error.json which means that the creating file name function went fine
            if file_name != "error.json":
                # check previous responses in output directory
                file_found = search_output_dir_for_previous_responses(file_name, "output/")

                # if file found load its content and put it in response
                if file_found:
                    with open(file_found) as previous_response_file:
                        json_response = json.load(previous_response_file)

                # if not found make new calculations
                else:
                    json_response = lof_calculation()

            # if file name is error.json make new calculation
            else:
                json_response = lof_calculation()

                # if request data is malfunctioning
        else:
            # logging to service.log and to screen
            logger.error("request.get_json(force=True)> looks request data is embty?")
            print "request.get_json(force=True)> looks request data is embty?"
            # adding error to response message
            json_response = error_resonse("request.get_json(force=True)> looks request data is embty?")


    # incase comiler catched an error
    except Exception, e:
        # logging error
        logger.error(e)
        # adding error to response message
        json_response = error_resonse(str(traceback.print_exc()))
        # printing full error trace
        traceback.print_exc()

    # returing service response to requesting client
    return json_response


""" adabtation of lof library"""


def lof_calculation():
    # try catch statemnt for error handling
    try:
        # start calculating as no previous responses cashed to fielsystem
        # checking parameters existance befor caculation
        # geting x axis used for plotting only not in calculation
        if responsee["x"]:
            x = responsee["x"]
            # geting y axis used for plotting only not in calculation
            if responsee["y"]:
                y = responsee["y"]
                print x
                print y

                """Retrieving Features Types"""

                if responsee["result"]["fields"]:
                    fields = responsee["result"]["fields"]
                    # logging feature types map
                    logger.debug('fields : ' + json.dumps(fields))

                    # initializing global varibels to hold features by  data type
                    # categorical datatype features list
                    global categorical_features_list
                    categorical_features_list = []

                    # numarical datatype features list
                    global numaric_features_list
                    numaric_features_list = []

                    # integer datatype features list
                    global int_features_list
                    int_features_list = []

                    # consuming provided features or auto detecting features if not provided
                    # check if variable exixts
                    if "analysisFeatures" in responsee:
                        features = responsee["analysisFeatures"]

                        # consuming provided features or auto detecting features if not provided
                        # check if it contatins values
                        if len(features) != 0:

                            for feature in features:
                                if feature:
                                    print "feature is:  " + feature
                                    for field in fields:
                                        if field["id"] == feature:
                                            if field["type"] == "text":
                                                categorical_features_list.append(field["id"])
                                            if field["type"] == "int4":
                                                # execlusing ID form features
                                                if field["id"] != "_id":
                                                    int_features_list.append(field["id"])
                                            if field["type"] == "numeric":
                                                numaric_features_list.append(field["id"])

                        # auto detecting features as not provided
                        else:
                            automatic_detection_of_features(fields)

                    # auto detecting features as not provided
                    else:
                        automatic_detection_of_features(fields)

                    # logging information of detected featues

                    logger.debug('features considred for LOF analysis are :  ')
                    logger.debug(categorical_features_list)
                    logger.debug(numaric_features_list)
                    logger.debug(int_features_list)

                    """Creating global pre-Analysis (with _ids) and Pre- Response
                    (this will keep track of original data) Data Set """
                    # initializing global variables
                    global analysis_ready_list
                    analysis_ready_list = []
                    global response_ready_full_list
                    response_ready_full_list = []

                    # acomodatibf global variables values

                    """DATA VALIDATION, TRANFORMATION AND HANDLING DUBLICATES AND NULLS STAGES """
                    for rec in records:
                        analysis_ready_list.append((rec["_id"],))
                        response_ready_full_list.append((rec["_id"],))

                    if len(categorical_features_list) > 0:
                        textual_to_numarical_categorical(records, categorical_features_list)

                    if len(int_features_list) > 0:
                        numeric_to_float(records, int_features_list)

                    if len(numaric_features_list) > 0:
                        numeric_to_float(records, numaric_features_list)

                    # logging information for accomdation results
                    logger.debug('analysis_ready_list : ' + json.dumps(analysis_ready_list))
                    logger.debug('response_ready_full_list : ' + json.dumps(response_ready_full_list))

                    """real_analysis_ready_data after removing _id as ID IS NOT USED FOR ANALYSIS"""
                    # the REMOVING IDS STAGE TO CREATE ANALYSIS DATASET
                    real_analysis_ready_data = [tuple(cols[1:]) for cols in analysis_ready_list]
                    # logging information for data going to analysis
                    logger.debug('analysis_ready_list : ' + json.dumps(analysis_ready_list))
                    logger.debug('real_analysis_ready_data: ' + json.dumps(real_analysis_ready_data))

                    """preparing LOF Model"""
                    # passing data to LOF model
                    lof = LOF(real_analysis_ready_data)

                    # preparing LOF anomaly reponse list
                    response_results_list = []

                    """Using LOF MODEL"""
                    # looping over values (tubles of readings) to get its anomaly score against the model
                    i = 0
                    for instance in real_analysis_ready_data:
                        # 10 is the number of nighbours considered for calculation
                        value = lof.local_outlier_factor(10, instance)
                        """
                        # Sending only outliers
                        # if(value>1):
                        """
                        # tagging readings with normal, local, and global based on its anomaly score
                        if i < len(records):
                            if value <= 1:
                                response_results_list.append(
                                    (value, records[i][x], records[i][y], "normal"))
                            if value > 1 and value <= 2:
                                response_results_list.append((value, records[i][x], records[i][y], "local"))
                            if value > 2:
                                response_results_list.append(
                                    (value, records[i][x], records[i][y], "global"))
                            i = i + 1

                    # initializing and preparing response
                    response_data = {}
                    response_data['success'] = True
                    response_data['result'] = []

                    # adding anomaly results messages
                    for record in response_results_list:
                        response_data['result'].append(record)

                    # adding service messages
                    response_data['messages'] = messages

                    # casting response map to json
                    json_response = json.dumps(response_data)
                    # logging infomation for the service client response
                    logger.debug('response : ' + json.dumps(response_data))
                    # storing new resonse to file system
                    store_responses_to_file_system("lof", json_response)

    # incase compiler catched an error
    except Exception, e:
        # logging error
        logger.error(e)
        # adding error to response message
        json_response = error_resonse(str(traceback.print_exc()))
        # printing full error trace
        traceback.print_exc()

    # returing service response to requesting client
    return json_response


"""dataset quality calculatioj function (nulls and dubloicates analysis)"""


def quality_calculation():
    # initializing global variables
    # service default response map
    response_data = {}
    response_data['success'] = False
    response_data['result'] = []
    # adding service messages
    response_data['messages'] = messages

    # try catch statemnt for error handling
    try:

        # start calculating as no previous responses cashed to fielsystem
        # checking parameters existance befor caculation
        # geting x axis used for plotting only not in calculation
        if responsee["x"]:
            x = responsee["x"]
            # geting y axis used for plotting only not in calculation
            if responsee["y"]:
                y = responsee["y"]
                print x
                print y

                """Retrieving Features Types"""

                if responsee["result"]["fields"]:
                    fields = responsee["result"]["fields"]
                    # logging feature types map
                    logger.debug('fields : ' + json.dumps(fields))

                    # initializing global varibels to hold features by  data type
                    # categorical datatype features list
                    global categorical_features_list
                    categorical_features_list = []

                    # numarical datatype features list
                    global numaric_features_list
                    numaric_features_list = []

                    # integer datatype features list
                    global int_features_list
                    int_features_list = []

                    # consuming provided features or auto detecting features if not provided
                    # check if variable exixts
                    if "analysisFeatures" in responsee:
                        features = responsee["analysisFeatures"]

                        # consuming provided features or auto detecting features if not provided
                        # check if it contatins values
                        if len(features) != 0:

                            for feature in features:
                                if feature:
                                    print "feature is:  " + feature
                                    for field in fields:
                                        if field["id"] == feature:
                                            if field["type"] == "text":
                                                categorical_features_list.append(field["id"])
                                            if field["type"] == "int4":
                                                # execlusing ID form features
                                                if field["id"] != "_id":
                                                    int_features_list.append(field["id"])
                                            if field["type"] == "numeric":
                                                numaric_features_list.append(field["id"])

                        # auto detecting features as not provided
                        else:
                            automatic_detection_of_features(fields)

                    # auto detecting features as not provided
                    else:
                        automatic_detection_of_features(fields)

                    # logging information of detected featues

                    logger.debug('features considred for quality analysis are :  ')
                    logger.debug(categorical_features_list)
                    logger.debug(numaric_features_list)
                    logger.debug(int_features_list)

                    """quality analusis simple logic"""

                    # quality analysis map initilization
                    quality = {}
                    measurment = {}
                    nulls = []
                    uniques = []
                    total = len(records)

                    # accomodating featues inside quality map

                    # accomodating categorical featues
                    if len(categorical_features_list) > 0:
                        for cat in categorical_features_list:
                            quality[cat] = []
                    # accomodating numarical featues
                    if len(numaric_features_list) > 0:
                        for cat in numaric_features_list:
                            quality[cat] = []

                    # accomodating integer featues rather than ID
                    if len(int_features_list) > 0:
                        for cat in int_features_list:
                            quality[cat] = []

                    """analyzing quality of categorical features"""
                    if len(categorical_features_list) > 0:
                        for cat in categorical_features_list:
                            seen = set()
                            for x in range(0, len(records)):

                                # nulls
                                if not records[x][cat]:
                                    nulls.append(x)

                                    # uniques
                                if records[x][cat] not in seen:
                                    uniques.append(x)
                                    seen.add(records[x][cat])

                                    # measurment map accomodation
                            measurment["nulls"] = nulls
                            measurment["uniques"] = uniques
                            measurment["totalReadings"] = total
                            measurment["nullCount"] = len(nulls)
                            measurment["uniqueCount"] = len(uniques)

                            # appending measurment map of current feature to the quality map
                            quality[cat] = measurment

                            # releasing maps and lists for next loop run
                            measurment = {}
                            nulls = []
                            uniques = []

                    """analyzing quality of numarical features"""
                    if len(numaric_features_list) > 0:
                        for cat in numaric_features_list:
                            seen = set()
                            for x in range(0, len(records)):

                                # nulls
                                if not records[x][cat]:
                                    nulls.append(x)
                                    # uniques
                                if records[x][cat] not in seen:
                                    uniques.append(x)
                                    seen.add(records[x][cat])

                            # measurment map accomodation
                            measurment["nulls"] = nulls
                            measurment["uniques"] = uniques
                            measurment["totalReadings"] = total
                            measurment["nullCount"] = len(nulls)
                            measurment["uniqueCount"] = len(uniques)

                            # appending measurment map of current feature to the quality map
                            quality[cat] = measurment

                            # releasing maps and lists for next loop run
                            measurment = {}
                            nulls = []
                            uniques = []

                    """analyzing quality of integers features"""
                    if len(int_features_list) > 0:
                        for cat in int_features_list:
                            seen = set()
                            for x in range(0, len(records)):

                                # nulls
                                if not records[x][cat]:
                                    nulls.append(x)
                                    # uniques
                                if records[x][cat] not in seen:
                                    uniques.append(x)
                                    seen.add(records[x][cat])

                            # measurment map accomodation
                            measurment["nulls"] = nulls
                            measurment["uniques"] = uniques
                            measurment["totalReadings"] = total
                            measurment["nullCount"] = len(nulls)
                            measurment["uniqueCount"] = len(uniques)

                            # appending measurment map of current feature to the quality map
                            quality[cat] = measurment

                            # releasing maps and lists for next loop run
                            measurment = {}
                            nulls = []
                            uniques = []

                    # setting new response values
                    response_data['success'] = True
                    # adding quality map to response
                    response_data['result'] = quality
                    # adding service messages
                    response_data['messages'] = messages

                    # casting response map to json
                    json_response = json.dumps(response_data)
                    # logging infomation for the service client response
                    logger.debug('response : ' + json.dumps(response_data))
                    # storing new resonse to file system
                    store_responses_to_file_system("quality", json_response)

    # incase comiler catched an error
    except Exception, e:
        # logging error
        logger.error(e)
        # adding error to response message
        json_response = error_resonse(str(traceback.print_exc()))
        # printing full error trace
        traceback.print_exc()

    # returing service response to requesting client
    return json_response


"""NULL detection function used for quality service"""


def detectNulls(feature):
    haveNulls = False
    for x in range(0, len(records)):

        if not records[x][feature]:
            haveNulls = True
            break

    return haveNulls


"""Dublicates detection function used for quality service"""


def detectDublicates(feature):
    allValuesDublicates = False
    seen = set()
    uniques = []

    for x in range(0, len(records)):

        if records[x][feature] not in seen:
            uniques.append(x)
            seen.add(records[x][feature])

    if len(uniques) == 1:
        allValuesDublicates = True

    return allValuesDublicates


""" textual / qualitative data transformation to quantitative / categorical data to match LOF requirments used by LOF service """


def textual_to_numarical_categorical(records, categorical_colls):
    # logging information
    logger.debug('def textual_to_numarical_categorical : ' + (str(categorical_colls)[1:-1]) + json.dumps(records))

    # try catch statemnt for error handling
    try:
        # parameters check
        # if records exisit
        if records:
            # if records exisit
            if categorical_colls:
                # if have values
                if len(categorical_colls) > 0:
                    # loop over categorical features values to detect nulls and duplicates
                    for coll in categorical_colls:

                        """ check duplicates """
                        # incase not all feature values are duplicates
                        if not detectDublicates(coll):

                            """ check nulls"""
                            # first geting full factorization
                            try:

                                """ factorization of textuall data stage full data to calculate median"""
                                categorical_list = []
                                for rec in records:
                                    if rec[coll]:
                                        categorical_list.append(rec[coll])
                                    else:
                                        categorical_list.append("NULL")
                                # numpy factorization of categorical data
                                a = np.array(categorical_list)
                                a_enc = pd.factorize(a)


                            # incase comiler catched an error
                            except Exception, e:
                                # logging error data
                                logger.error(e)

                            # incase no null detected
                            if not detectNulls(coll):

                                # logging factorization information
                                logger.debug(
                                    'Categorical feature ( ' + coll + ' ) factorization stage all values ( ' + str(
                                        len(a_enc[0])) + ' ) : ')
                                logger.debug(a_enc[0])
                                logger.debug(
                                    'Categorical feature ( ' + coll + ' )  factorization stage unique values ( ' + str(
                                        len(a_enc[1])) + ' ) : ')
                                logger.debug(a_enc[1])

                                # accomdating factorized textual feature values to analysis dataset
                                for x in range(0, len(analysis_ready_list)):
                                    analysis_ready_list[x] = analysis_ready_list[x] + (a_enc[0][x],)
                                    response_ready_full_list[x] = response_ready_full_list[x] + (
                                    a_enc[0][x], a_enc[1][a_enc[0][x]],)


                            # incase  nulls detected
                            else:

                                # null processing

                                # getting not null values to use it in median calculation
                                notnulls = []

                                for x in xrange(0, len(records)):
                                    if records[x][coll]:
                                        notnulls.append(records[x][coll])

                                # special processign for textual values before mean calculation
                                a_of_notnulls = np.array(notnulls)
                                a_enc_of_notnulls = pd.factorize(a_of_notnulls)

                                factorizedList = []
                                for f in a_enc_of_notnulls[0]:
                                    factorizedList.append(f)

                                if len(factorizedList) > 0:
                                    # median calculation
                                    median = np.median(factorizedList)

                                # filling empty celss with median value
                                for x in xrange(0, len(analysis_ready_list)):

                                    if records[x][coll]:

                                        analysis_ready_list[x] = analysis_ready_list[x] + (a_enc[0][x],)
                                        response_ready_full_list[x] = response_ready_full_list[x] + (
                                            a_enc[0][x], a_enc[1][a_enc[0][x]],)

                                    else:

                                        analysis_ready_list[x] = analysis_ready_list[x] + (float(median),)
                                        response_ready_full_list[x] = response_ready_full_list[x] + (float(median),)

                                # storing algorithem decesions
                                message = "feature ( " + coll + " ) had NULL values and median of " + str(
                                    median) + " is used instead to fill those places!"
                                # appending message to global messages list
                                messages.append(message)
                                # logging information
                                logger.debug(message)



                        # incase  all feature values are duplicates
                        else:
                            # storing algorithem decesions
                            message = "feature ( " + coll + " ) values are all duplicates!"
                            # appending message to global messages list
                            messages.append(message)
                            # logging information
                            logger.debug(message)

    # incase compiler catched an error
    except Exception, e:
        # logging error
        logger.error(e)
        # printing full error trace
        traceback.print_exc()


""" numarical data transformation to quantitative / float data to match LOF requirments used by LOF service """


def numeric_to_float(records, numeric_colls):
    # logging information
    logger.debug('def numeric_to_float : ' + (str(numeric_colls)[1:-1]) + json.dumps(records))

    # try catch statemnt for error handling
    try:
        # parameters check
        # if records exisit
        if records:
            # if records exisit
            if numeric_colls:
                # if have values
                if len(numeric_colls) > 0:
                    # loop over  features values to detect nulls and duplicates
                    for coll in numeric_colls:
                        if coll:

                            """ check duplicates """
                            # incase not all feature values are duplicates
                            if not detectDublicates(coll):

                                """ check nulls"""
                                # incase no null detected
                                if not detectNulls(coll):

                                    # normal processing logic
                                    for x in range(0, len(analysis_ready_list)):

                                        # accomdating feature values to analysis dataset
                                        if records[x][coll]:
                                            print float(records[x][coll])
                                            analysis_ready_list[x] = analysis_ready_list[x] + (float(records[x][coll]),)
                                            response_ready_full_list[x] = response_ready_full_list[x] + (
                                            float(records[x][coll]),)

                                # incase  nulls detected
                                else:

                                    # null processing

                                    # getting not null values to use it in median calculation
                                    notnulls = []

                                    for x in range(0, len(records)):
                                        if records[x][coll]:
                                            notnulls.append(float(records[x][coll]))
                                    if len(notnulls) > 0:
                                        # median calculation
                                        median = np.median(notnulls)

                                    # filling empty celss with median value
                                    for x in range(0, len(analysis_ready_list)):

                                        if records[x][coll]:
                                            print float(records[x][coll])
                                            analysis_ready_list[x] = analysis_ready_list[x] + (float(records[x][coll]),)
                                            response_ready_full_list[x] = response_ready_full_list[x] + (
                                            float(records[x][coll]),)
                                        else:

                                            analysis_ready_list[x] = analysis_ready_list[x] + (float(median),)
                                            response_ready_full_list[x] = response_ready_full_list[x] + (float(median),)

                                    # storing algorithem decesions
                                    message = "feature ( " + coll + " ) had NULL values and median of " + str(
                                        median) + " is used instead to fill those places!"
                                    # appending message to global messages list
                                    messages.append(message)
                                    # logging information
                                    logger.debug(message)



                            # incase  all feature values are duplicates
                            else:
                                # storing algorithem decesions
                                message = "feature ( " + coll + " ) values are all duplicates!"
                                # appending message to global messages list
                                messages.append(message)
                                # logging information
                                logger.debug(message)

    # incase compiler catched an error
    except Exception, e:
        # logging error
        logger.error(e)
        # printing full error trace
        traceback.print_exc()


""" integer NON ID data handling to match LOF requirments used by LOF service """
def int_non_id_adding(records, int_colls):
    # logging information
    logger.debug('def textual_to_numarical_categorical : ' + (str(int_colls)[1:-1]) + json.dumps(records))

    # try catch statemnt for error handling
    try:
        # parameters check
        # if records exisit
        if records:
            # if records exisit
            if int_colls:
                # if have values
                if len(int_colls) > 0:
                    # loop over  features values to detect nulls and duplicates
                    for coll in int_colls:
                        if coll:
                            print coll

                            """ check duplicates """
                            # incase not all feature values are duplicates
                            if not detectDublicates(coll):

                                """ check nulls"""
                                # incase no null detected
                                if not detectNulls(coll):

                                    # normal processing logic
                                    for x in range(0, len(analysis_ready_list)):

                                        # accomdating feature values to analysis dataset
                                        if records[x][coll]:
                                            print float(records[x][coll])
                                            analysis_ready_list[x] = analysis_ready_list[x] + (float(records[x][coll]),)
                                            response_ready_full_list[x] = response_ready_full_list[x] + (
                                                float(records[x][coll]),)

                                # incase  nulls detected
                                else:

                                    # null processing

                                    # getting not null values to use it in median calculation
                                    notnulls = []

                                    for x in range(0, len(records)):
                                        if records[x][coll]:
                                            notnulls.append(records[x][coll])
                                    if len(notnulls) > 0:
                                        # median calculation
                                        median = np.median(notnulls)

                                    # filling empty celss with median value
                                    for x in range(0, len(analysis_ready_list)):

                                        if records[x][coll]:
                                            print float(records[x][coll])
                                            analysis_ready_list[x] = analysis_ready_list[x] + (records[x][coll],)
                                            response_ready_full_list[x] = response_ready_full_list[x] + (
                                                float(records[x][coll]),)
                                        else:

                                            analysis_ready_list[x] = analysis_ready_list[x] + (float(median),)
                                            response_ready_full_list[x] = response_ready_full_list[x] + (float(median),)

                                    # storing algorithem decesions
                                    message = "feature ( " + coll + " ) had NULL values and median of " + str(
                                        median) + " is used instead to fill those places!"
                                    # appending message to global messages list
                                    messages.append(message)
                                    # logging information
                                    logger.debug(message)

                            # incase  all feature values are duplicates
                            else:
                                # storing algorithem decesions
                                message = "feature ( " + coll + " ) values are all duplicates!"
                                # appending message to global messages list
                                messages.append(message)
                                # logging information
                                logger.debug(message)


    # incase compiler catched an error
    except Exception, e:
        # logging error
        logger.error(e)
        # printing full error trace
        traceback.print_exc()


"""automatic_detection_of_features function used in quality and lof service when analysis features are not provided"""
def automatic_detection_of_features(fields):
    if fields:

        for field in fields:
            if field["type"] == "text":
                categorical_features_list.append(field["id"])
            if field["type"] == "int4":
                if field["id"] != "_id":
                    int_features_list.append(field["id"])
            if field["type"] == "numeric":
                numaric_features_list.append(field["id"])


"""store_responses_to_file_system function used in quality and lof service to implement file system caching"""
def store_responses_to_file_system(sender, json_response):

    logger.info('cashing to file system.')
    path = "output/"
    file_name = creating_file_name(sender)
    import os
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as exception:
            logger.error(exception)
    with open(path + file_name, 'w+') as outfile:
        json.dump(json_response, outfile)

"""search_output_dir_for_previous_responses function used in quality and lof service to chick previous file system cached responses"""
def search_output_dir_for_previous_responses(file_name, path):
    import os

    for root, dirs, files in os.walk(path):
        if file_name in files:
            return str(os.path.join(root, file_name))

"""creating_file_name function used in quality and lof service to create name of the file caching the responses"""
def creating_file_name(sender):
    # storing responses to file system for faster response time
    #setting default variables values
    features_names = ""
    json_response = "error.json"
    # try catch statemnt for error handling
    try:
        # incase features are provided
        if 'analysisFeatures' in responsee and responsee['analysisFeatures']:
            if len(responsee['analysisFeatures']) > 0:
                for feature in responsee['analysisFeatures']:
                    if len(feature) > 0:
                        features_names += "_" + str(feature)
            if responsee['result']['resource_id']:
                file_name = str(responsee['result']['resource_id']) + features_names + "_" + sender + ".json"

        # incase features are not provided
        else:

            if responsee['result']['resource_id']:
                file_name = str(responsee['result']['resource_id']) + "_" + sender + ".json"

        # logging file name information
        logger.info(file_name.replace(" ", "_"))
        json_response = file_name.replace(" ", "_")


    # incase compiler catched an error
    except Exception, e:
        # logging error
        logger.error(e)
        # printing full error trace
        traceback.print_exc()

    return json_response

"""error_resonse function used in quality and lof service to create error responses"""
def error_resonse(excp):
    json_response = json.dumps({'success': False, 'error': excp, 'messages': messages})
    logger.debug('response : ' + json.dumps(json_response))
    return json_response


"""Server host and port configuration"""
if __name__ == "__main__":
    app.run(
        #host="127.0.0.1",
        #port=int("5000")
        host = "vmrtpa05.deri.ie",
        port = int("8003")
    )
