# -*- encoding: utf-8 -*-

import boto3

s3 = boto3.client('s3')

def createBucket(ACL, Bucket, CreateBucketConfiguration):
    return s3.create_bucket(
        ACL=ACL,
        Bucket=Bucket,
        CreateBucketConfiguration=CreateBucketConfiguration
    )

def deleteBucket(Bucket):   
    return s3.delete_bucket(Bucket=Bucket)

def headBucket(Bucket):
    return s3.head_bucket(Bucket=Bucket)

def listObject(Bucket):
    return s3.list_objects(Bucket=Bucket)

# 创建目录
# 做为对象存储，s3 的数据是一种 kv 的结构。目录其实就是 key 以 / 结尾的特殊对象
# 如果需要开启版本 和 MFA 的支持，需要另外一个函数 put_bucket_versioning
def createDir(ACL, Bucket, Key, StorageClass):
    return s3.put_object(
        ACL=ACL,
        Bucket=Bucket,
        ContentLength=0,
        Key=Key,
        StorageClass=StorageClass
    )

# 删除目录
# 同上，删除目录其实就是删除一个对象。开启 MFA 需要在创建对象之后添加 version 等属性
# 如果目录不为空，删除会失败，所以需要加一个遍历才行。
def deleteDir(Bucket, Key):
    if Key.endswith("/"):
        objects = s3.list_objects(
            Bucket=Bucket,
            Prefix=Key,
            Delimiter="/"
        )

        result = []

        if "Contents" in objects:
            for obj in objects["Contents"]:
                result.append(s3.delete_object(
                    Bucket=Bucket,
                    Key=obj["Key"]
                ))
        return result
    else:
        raise Exception("It is not a directory!")

# 上传文件
# 有两种方式 put_ojbect 和 upload_file
# 前者可以添加多种权限参数，后者更精确的控制文件传输
# 是不是可以考虑两者配合？一个负责创建对象，一个负责上传文件。图方便就直接put_object
# https://stackoverflow.com/questions/38442512/difference-between-upload-and-putobject-for-uploading-a-file-to-s3
# https://gist.github.com/veselosky/9427faa38cee75cd8e27
def uploadFile(ACL,Body,Bucket,Key,ContentType,StorageClass):
    return s3.put_object(
        ACL=ACL,
        Body=Body,
        Bucket=Bucket,
        Key=Key,
        ContentType=ContentType,
        StorageClass=StorageClass
    )

# 下载文件
def downloadFile(Bucket,Key):
    return s3.get_object(
        Bucket=Bucket,
        Key=Key
    )

# 删除文件
def deleteFile(Bucket, Key):
    return s3.delete_object(
        Bucket=Bucket,
        Key=Key
    )