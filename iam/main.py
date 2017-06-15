
import boto3

iam = boto3.client("iam")

def createRole(RoleName, Path, AssumeRolePolicyDocument, Description):
    return iam.create_role(
        RoleName=RoleName,
        Path=Path,
        AssumeRolePolicyDocument=AssumeRolePolicyDocument,
        Description=Description
    )

def deleteRole(RoleName):
    return iam.delete_role(RoleName=RoleName)

def getRole(RoleName):
    return iam.get_role(RoleName=RoleName)

def putRolePolicy(RoleName, PolicyName, PolicyDocument):
    return iam.put_role_policy(
        RoleName=RoleName,
        PolicyName=PolicyName,
        PolicyDocument=PolicyDocument
    )

def delRolePolicy(RoleName, PolicyName):
    return iam.delete_role_policy(
        RoleName=RoleName,
        PolicyName=PolicyName
    )