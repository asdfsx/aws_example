
import boto3

lambdaobj = boto3.client("lambda")

def createFunction(FunctionName, Runtime, Role, Handler, Code, Timeout):
    return lambdaobj.create_function(
        FunctionName=FunctionName,
        Runtime=Runtime,
        Role=Role,
        Handler=Handler,
        Code=Code,
        Timeout=Timeout
    )

def deleteFcuntion(FunctionName):
    return lambdaobj.delete_function(FunctionName=FunctionName)

def getFunction(FunctionName):
    return lambdaobj.get_function(FunctionName=FunctionName)

def createEventSourceMapping(EventSourceArn, FunctionName, Enabled, BatchSize, StartingPosition):
    return lambdaobj.create_event_source_mapping(
        EventSourceArn=EventSourceArn,
        FunctionName=FunctionName,
        Enabled=Enabled,
        BatchSize=BatchSize,
        StartingPosition=StartingPosition,
    )
