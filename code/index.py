import sys
import logging
import pymysql
import json
import os

#rds settings
rds_endpoint = os.environ['rds_endpoint']
username=os.environ['username']
password=os.environ['password']
db_name=os.environ['db_name']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Connection
try:
    connection = pymysql.connect(host=rds_endpoint, user=username,
        passwd=password, db=db_name)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()
logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

def handler(event, context):
    cur = connection.cursor()  
## Retrieve Data
    ## Check if already in Queue
    query = "SELECT status, branchId FROM Queue WHERE customerId={}".format(event['customerId'])
    cur.execute(query)
    connection.commit()
    rows = cur.fetchall()
    inQueue = False
    branchId = None
    for row in rows:
        print(row[0])
        if row[0] == "Q" or row[0] == "D" or row[0] == "P":
            inQueue = True
            branchId = row[1]
            query = "SELECT name FROM Branch where id={}".format(branchId)        
            cur.execute(query)
            connection.commit()
            branchName = cur.fetchone()[0]
            print("for loop: " + branchName)
    ## Insert if not in Queue
    if(not inQueue):
        print("not in queue 1")
        query = "SELECT name FROM Branch where id={}".format(event['branchId'])        
        cur.execute(query)
        branchName = cur.fetchone()[0]
        query = "SELECT MAX(queueNumber) FROM Queue WHERE branchId={}".format(event['branchId'])
        cur.execute(query)
        queueNumber= cur.fetchone()[0]
        if queueNumber is None:
            queueNumber = 1
        else:
            queueNumber +=1
        print("queueNumber: ", queueNumber)
        query = "INSERT INTO Queue(status,queueNumber,createdDT,customerId,branchId) VALUES('Q','{}',now(),'{}','{}')".format(queueNumber, event['customerId'], event['branchId'])
        cur.execute(query)
        connection.commit()
        print(cur.rowcount, "record(s) affected")
## Construct body of the response object
    transactionResponse = {}
# Construct http response object
    responseObject = {}
    # responseObject['statusCode'] = 200
    # responseObject['headers'] = {}
    # responseObject['headers']['Content-Type']='application/json'
    # responseObject['headers']['Access-Control-Allow-Origin']='*'
    if(not inQueue):
        responseObject['data'] = json.dumps(transactionResponse, sort_keys=True,default=str)
    else:
         responseObject['data'] = {'error':'Already in queue for Branch ' + str(branchId) + ': ' + branchName, 'branchId': branchId, 'branchName': branchName}
    
    #k = json.loads(responseObject['body'])
    #print(k['uin'])

    return responseObject