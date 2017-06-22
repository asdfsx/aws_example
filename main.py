# -*- encoding: utf-8 -*-

from string import Template
import time
import traceback
import zipfile
import os.path

import boto3
import sns
import dynamodb
import iam
import lambdaa
import s3


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
                "ec2:CreateNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DeleteNetworkInterface"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::*"
            ]
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

s3_bucketname = "boto3-s3-example" # 名字格式有要求，不能有下划线
s3_dirname = "boto3-test/"
s3_uploadfile = "zips/boto3_lambda_example.zip"

tablename = "boto3_dynamodb_example"
rolename = "boto3_role_example"
policyname = "boto3_policy_example"
functioname = "boto3_lambda_example"
functionalias = "boto3_lambda_alias"
runtime = "python2.7"
handler = "boto3_lambda_example.handler"
pythonfilename = "zips/boto3_lambda_example.py"
zipfilename = "zips/boto3_lambda_example.zip"

def preparehandler(topicarn):
    print handler_template.substitute(topicarn=topicarn)
    with open(pythonfilename, "w") as ostream:
        ostream.write(handler_template.substitute(topicarn=topicarn))
    with zipfile.ZipFile(zipfilename, "w") as myzip:
        myzip.write(pythonfilename, os.path.basename(pythonfilename))

def main():
    # 获取 region
    region = "us-west-2"
    # 获取 accountid
    client = boto3.client("sts")
    accountid = client.get_caller_identity()["Account"]

    # 查询 s3 是否存在
    bucket = None
    try:
        bucket = s3.headBucket(s3_bucketname)
        print bucket
    except:
        print traceback.format_exc()

    # 创建 S3
    if bucket == None:
        newbucket = s3.createBucket(
            ACL="private",
            Bucket=s3_bucketname,
            CreateBucketConfiguration={
                "LocationConstraint":"us-west-2"
            }
        )
        print newbucket

    # 列出所有的 object
    objects = s3.listObject(s3_bucketname)
    print objects

    # 创建目录
    newdir = s3.createDir(
        ACL="private",
        Bucket=s3_bucketname,
        Key=s3_dirname,
        StorageClass="STANDARD"
    )
    print newdir
    # 上传文件
    with open(s3_uploadfile) as istream:
        upload = s3.uploadFile(
            ACL="private",
            Body=istream.read(),
            Bucket=s3_bucketname,
            Key=s3_dirname + os.path.basename(s3_uploadfile),
            ContentType="application/zip",
            StorageClass="STANDARD"
        )
        print upload

    # 下载文件
    download = s3.downloadFile(
        Bucket=s3_bucketname,
        Key=s3_dirname + os.path.basename(s3_uploadfile)
    )

    with open("test.zip","w") as ostream:
        ostream.write(download["Body"].read())

    # 删除文件
    # response = s3.deleteFile(
    #     Bucket=s3_bucketname,
    #     Key=s3_dirname + os.path.basename(s3_uploadfile)
    # )
    # 删除目录
    # response = s3.deleteDir(
    #      Bucket=s3_bucketname,
    #      Key=s3_dirname
    #  )
    # print response

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
        #preparehandler(newtopic["TopicArn"])

        byte_stream = None
        with open(zipfilename) as f_obj:
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
            Timeout=5,
            Publish=False,
            VpcConfig={
                'SubnetIds': [
                    'subnet-1e125857',
                ],
                'SecurityGroupIds': [
                    'sg-944e70ef',
                ]
            },
        )
        lambdaobj = newlambda
    print lambdaobj

    # 修改后，lambda 创建后不再是 publish 的，需要单独 publish version
    publish = lambdaa.publishVersion(
        FunctionName=functioname,
        Description=functionalias
    )
    print publish

    # publish 后，对新的version 添加别名
    lambdaobj = None
    try:
        lambdaobj = lambdaa.getAlias(
            FunctionName=functioname,
            Name=functionalias
        )
    except:
        print traceback.format_exc()

    if lambdaobj == None:
        lambdaobj = lambdaa.createAlias(
            FunctionName=functioname,
            Name=functionalias,
            FunctionVersion=publish["Version"],
            Description=functionalias
        )
    else:
        lambdaobj = lambdaa.updateAlias(
            FunctionName=functioname,
            Name=functionalias,
            FunctionVersion=publish["Version"],
            Description=functionalias
        )


    # 从 s3 上添加一个 function
    # 查询 lambda 是否存在
    lambdaobj = None
    try:
        lambdaobj = lambdaa.getFunction(functioname+"2")
    except:
        print traceback.format_exc()

    if lambdaobj is None:
        #preparehandler(newtopic["TopicArn"])

        byte_stream = None
        with open(zipfilename) as f_obj:
            byte_stream = f_obj.read()

        if byte_stream is None:
            return

        # 添加 lambda
        newlambda = lambdaa.createFunction(
            FunctionName=functioname+"2",
            Runtime=runtime,
            Role=roleobj["Role"]["Arn"],
            Handler=handler,
            Code={
                "S3Bucket": s3_bucketname,
                "S3Key": s3_dirname + os.path.basename(s3_uploadfile),
            },
            Timeout=5,
            Publish=False,
            VpcConfig={}
        )
        lambdaobj = newlambda
    print lambdaobj

    # 检查映射是否创建
    mappings = lambdaa.listEventSourceMappings(
        EventSourceArn=tableobj["Table"]["LatestStreamArn"],
        FunctionName=functioname
    )
    print mappings

    if len(mappings["EventSourceMappings"]) == 0:
        # 设置 event source 到 lambda function
        mapping = lambdaa.createEventSourceMapping(
            EventSourceArn=tableobj["Table"]["LatestStreamArn"],
            FunctionName=functioname,
            Enabled=True,
            BatchSize=1,
            StartingPosition="TRIM_HORIZON",
        )
        print mapping

    time.sleep(5)

    # 向 Dynamodb 中写入数据，触发事件
    result = dynamodb.putItem(
        TableName=tablename,
        Item={
            "Username" : {"S" : "asdfsx"},
            "Timestamp" : {"S" : "2017-06-15"},
            "Message" : {"S" : "message from python"}
        }
    )
    print result

if __name__ == "__main__":
    main()
