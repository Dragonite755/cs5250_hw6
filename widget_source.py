from abc import ABC, abstractmethod
import boto3

def source_factory(source_type, name):
    if source_type == "request-bucket":
        return BucketSource(name)
    return None

class RequestSource():
    @abstractmethod
    def get_request():
        pass
        
class BucketSource(RequestSource):
    def __init__(self, bucket_name):
        self.__bucket = s3.Bucket(bucket_name)
        
    def get_request(self):
        objects = list(self.__bucket.objects.limit(count=1))
        print(objects)