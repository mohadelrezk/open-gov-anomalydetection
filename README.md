## requirments

flask
flask_cors

installation:

virtualenv venv
pip install -r requirments.txt

-------------------------------------------------------
###Running the service:


1-Start mongod:

> sudo mongod start

2- ##First time run


A- Spotlight:

> cd ~/workspace/rtpa-dbpedia-spotlight

> java -Xmx5G -Xms5G -jar dbpedia-spotlight-latest.jar en http://localhost:2222/rest


> java -jar dbpedia-spotlight-latest.jar en http://srvgal102.deri.ie:8005/rest


> screen -S spotlight java -Xmx5G -Xms5G -jar dbpedia-spotlight-latest.jar en http://localhost:2222/rest

B- (collecting and analyzing CKAN instance data) FULL PIPELINE:

> python collect-Network-components.py 'http://data.gov.ie' 'localhost' 27017 'rtpa' 'datasetIDS' 'rtpaF' 0 True


3- run websevice and visulization:

> source venv/bin/activate

> python rtpa-publisher-network-builder.py




#### Usages:

### Full Pipline Execution

>> python collect-Network-components.py 'http://data.gov.ie' 'localhost' 27017 'rtpa' 'datasetIDS' 'rtpaF' 0 True

arg 1: open data portal (ckan) ex: http://data.gov.ie

arg 2: mongodb host ex: localhost

arg 3: mongodb port ex: 27017 (int)

arg 4: database name ex: rtpa

arg 5: collection name to store dataset ids ex: datasetIDS

arg 6: colection name to store dataset catalogues and related analysis data ex: rtpaF

arg 7: (Optional) start from index in case collection was interupted ex: 2000, default is 0 (int)

arg 8: (Optional) Run Named Entity Recognition process or not ex: True, default False

arg 9: (Optional) Build Network or not ex: net, By default it will not run unless requested
