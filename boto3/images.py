import boto3

client = boto3.client('ec2')
response = client.describe_images(
    ImageIds=["ami-6f68cf0f"],
    Filters=[
        {"Name":"is-public", "Values":["true"]},
        {"Name":"architecture", "Values":["x86_64"]},
    ]
)
print response

