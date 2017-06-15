import boto3

dynamodb = boto3.client("dynamodb")

def createTable(TableName, AttributeDefinitions, KeySchema, ProvisionedThroughput, StreamSpecification):
    return dynamodb.create_table(
        TableName = TableName,
        AttributeDefinitions = AttributeDefinitions,
        KeySchema = KeySchema,
        ProvisionedThroughput = ProvisionedThroughput,
        StreamSpecification = StreamSpecification
    )

def deleteTable(TableName):
    return dynamodb.delete_table(TableName=TableName)

def describeTable(TableName):
    return dynamodb.describe_table(TableName=TableName)

def putItem(TableName, Item):
    return dynamodb.put_item(
        TableName=TableName,
        Item=Item
    )