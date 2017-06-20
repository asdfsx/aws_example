
import boto3

lambdaobj = boto3.client("lambda")

def createFunction(FunctionName, Runtime, Role, Handler, Code, Timeout, Publish):
    return lambdaobj.create_function(
        FunctionName=FunctionName,
        Runtime=Runtime,
        Role=Role,
        Handler=Handler,
        Code=Code,
        Timeout=Timeout,
        Publish=Publish
    )


def deleteFcuntion(FunctionName):
    return lambdaobj.delete_function(FunctionName=FunctionName)


def getFunction(FunctionName):
    return lambdaobj.get_function(FunctionName=FunctionName)


def publishVersion(FunctionName,Description):
    return lambdaobj.publish_version(
        FunctionName=FunctionName,
        Description=Description
    )


def createAlias(FunctionName, FunctionVersion, Name, Description):
    return lambdaobj.create_alias(
        FunctionName=FunctionName,
        Name=Name,
        FunctionVersion=FunctionVersion,
        Description=Description
    )


def updateAlias(FunctionName, FunctionVersion, Name, Description):
    return lambdaobj.create_alias(
        FunctionName=FunctionName,
        Name=Name,
        FunctionVersion=FunctionVersion,
        Description=Description
    )


def getAlias(FunctionName, Name):
    return client.get_alias(
        FunctionName=FunctionName,
        Name=Name
    )


def createEventSourceMapping(EventSourceArn, FunctionName, Enabled, BatchSize, StartingPosition):
    return lambdaobj.create_event_source_mapping(
        EventSourceArn=EventSourceArn,
        FunctionName=FunctionName,
        Enabled=Enabled,
        BatchSize=BatchSize,
        StartingPosition=StartingPosition,
    )


def listEventSourceMappings(EventSourceArn, FunctionName):
    return lambdaobj.list_event_source_mappings(
        EventSourceArn=EventSourceArn,
        FunctionName=FunctionName
    )
