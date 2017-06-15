import boto3

sns = boto3.client('sns')

def createTopic(topic_name):
    return sns.create_topic(Name=topic_name)

def listTopic():
    topics = sns.list_topics()
    return topics["Topics"]

def deleteTopic(topicArn):
    return sns.delete_topic(TopicArn=topicArn)

def subscribe(TopicArn, Protocol, Endpoint):
    return sns.subscribe(
        TopicArn=TopicArn,
        Protocol=Protocol,
        Endpoint=Endpoint
    )
