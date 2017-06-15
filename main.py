# -*- encoding: utf-8 -*-

import boto3
import sns
import dynamodb
import iam
import lambdaa
import traceback
import zipfile
from string import Template

policy_template = Template("""{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "lambda:InvokeFunction",
            "Resource": "arn:aws:lambda:${region}:${accountid}:function:${functioname}*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:${region}:${accountid}:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
                "dynamodb:ListStreams"
            ],
            "Resource": "arn:aws:dynamodb:${region}:${accountid}:table/${tablename}/stream/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "sns:Publish"
            ],
            "Resource": [
                "*"
            ]
        }
    ]
}""")


handler_template=Template("""
# -*- coding: utf-8 -*-

import boto3

sns = boto3.client("sns")

def handler(event, context):
    # Your code goes here!
    print event
    for record in event["Records"]:
        print "Stream record: ", record
        if record['eventName'] == "INSERT":
            who = record["dynamodb"]["NewImage"]["Username"]["S"]
            when = record["dynamodb"]["NewImage"]["Timestamp"]["S"]
            what = record["dynamodb"]["NewImage"]["Message"]["S"]

            sns.publish(
                TopicArn="${topicarn}",
                Subject="A new record username is " + who, 
                Message="boto3_dynamodb_example create by template add a new record: Username " + who + " Timestamp " + when + ":\\n\\n Message:" + what,
            )
    return {"Message" : "Successfully processed %s Records" % len(event["Records"])}

""")

def preparehandler(topicarn):
    print handler_template.substitute(topicarn=topicarn)
    with open("zips/boto3_lambda_example.py", "w") as ostream:
        ostream.write(handler_template.substitute(topicarn=topicarn))
    with zipfile.ZipFile("zips/boto3_lambda_example.zip", "w") as myzip:
        myzip.write("zips/boto3_lambda_example.py", "boto3_lambda_example.py")

def main():
    # 获取 region
    region = "us-west-2"
    # 获取 accountid
    client = boto3.client("sts")
    accountid = client.get_caller_identity()["Account"]
    tablename = "boto3_dynamodb_example"
    rolename = "boto3_role_example"
    policyname = "boto3_policy_example"
    functioname = "boto3_lambda_example"
    runtime = "python2.7"
    handler = "boto3_lambda_example.handler"
    zipfile = "zips/boto3_lambda_example.zip"

    # 创建 sns topic
    newtopic = sns.createTopic("boto3_sns_example")
    print newtopic

    # sns subscribe
    subscribe_result = sns.subscribe(
        TopicArn=newtopic["TopicArn"],
        Protocol="email",
        Endpoint="asdfsx@gmail.com"
    )
    print subscribe_result

    # 查询 dynamodb 是否存在
    tableobj = None
    try:
        tableobj = dynamodb.describeTable(TableName=tablename)
    except:
        print traceback.format_exc()

    # 创建 dynamodb with stream
    if tableobj is None:
        newtable = dynamodb.createTable(
            TableName=tablename,
            AttributeDefinitions=[
                {"AttributeName": "Username", "AttributeType": "S"},
                {"AttributeName": "Timestamp", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "Username", "KeyType": "HASH"},
                {"AttributeName": "Timestamp", "KeyType": "RANGE"},
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits" : 1,
                "WriteCapacityUnits" : 1
            },
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_IMAGE",
            }
        )
        tableobj = newtable
    print tableobj

    # 查询 role 是否存在
    roleobj = None
    try:
        roleobj = iam.getRole(RoleName=rolename)
    except:
        traceback.format_exc()

    if roleobj is None:
        # 创建 role
        newrole = iam.createRole(
            RoleName=rolename,
            Path="/service-role/",
            AssumeRolePolicyDocument="""{
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Principal": {
             "Service": "lambda.amazonaws.com"
           },
           "Action": "sts:AssumeRole"
         }
       ]
     }""",
            Description="boto3_role_example"
        )
        roleobj = newrole
    print roleobj

    # 添加 policy
    newpolicy = iam.putRolePolicy(
        RoleName=rolename,
        PolicyName=policyname,
        PolicyDocument=policy_template.substitute(
            region=region,
            accountid=accountid,
            tablename=tablename,
            functioname=functioname,
        ),
    )
    print newpolicy


    # 查询 lambda 是否存在
    lambdaobj = None
    try:
        lambdaobj = lambdaa.getFunction(functioname)
    except:
        print traceback.format_exc()

    if lambdaobj is None:
        preparehandler(newtopic["TopicArn"])

        byte_stream = None
        with open(zipfile) as f_obj:
            byte_stream = f_obj.read()

        if byte_stream is None:
            return

        # 添加 lambda
        newlambda = lambdaa.createFunction(
            FunctionName=functioname,
            Runtime=runtime,
            Role=roleobj["Role"]["Arn"],
            Handler=handler,
            Code={
                'ZipFile': byte_stream,
            },
            Timeout=5
        )
        lambdaobj = newlambda
    print lambdaobj

    # 设置 event source 到 lambda function
    mapping = lambdaa.createEventSourceMapping(
        EventSourceArn=tableobj["Table"]["LatestStreamArn"],
        FunctionName=functioname,
        Enabled=True,
        BatchSize=1,
        StartingPosition="TRIM_HORIZON",
    )
    print mapping

if __name__ == "__main__":
    main()
