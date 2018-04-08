
# -*- coding: utf-8 -*-

import boto3
import mysql.connector

sns = boto3.client("sns")

def handler(event, context):
    # Your code goes here!
    print event
    print "mysql==============="
    for record in event["Records"]:
        print "Stream record: ", record
        if record['eventName'] == "INSERT":
            who = record["dynamodb"]["NewImage"]["Username"]["S"]
            when = record["dynamodb"]["NewImage"]["Timestamp"]["S"]
            what = record["dynamodb"]["NewImage"]["Message"]["S"]

            sns.publish(
                TopicArn="arn:aws:sns:us-west-2:451299163191:boto3_sns_example",
                Subject="A new record username is " + who, 
                Message="boto3_dynamodb_example create by template add a new record: Username " + who + " Timestamp " + when + ":\n\n Message:" + what,
            )
    print "mysql==============="
    print "mysql==============="
    print "mysql==============="
    # Connect with the MySQL Server
    cnx = mysql.connector.connect(
        user='root',
        password='root',
        host='ip-10-0-0-201.us-west-2.compute.internal',
        port=3306,
        database='mysql')
    curA = cnx.cursor(buffered=True)
    query = "SELECT * FROM mysql.user limit 1"
    curA.execute(query)
    for record in curA:
        print record
    cnx.close()
    return {"Message" : "Successfully processed %s Records" % len(event["Records"])}
