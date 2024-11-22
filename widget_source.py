from abc import ABC, abstractmethod
import json
import boto3

class RequestSource(ABC):
    """
    Read first request found in source and delete it
    Return JSON of request if found, None otherwise
    """
    @abstractmethod
    def poll_request(self):
        pass
        
class BucketSource(RequestSource):
    def __init__(self, bucket_name):
        s3 = boto3.resource("s3")
        self.__bucket = s3.Bucket(bucket_name)
        
    def poll_request(self):
        try:
            objects = list(self.__bucket.objects.limit(count=1))
            if not objects:
                return
            request = objects[0]
                
            json_string = request.get()["Body"].read().decode("utf-8")
            data = json.loads(json_string)
            
            request.delete()
            return data
        except Exception as e:
            print(e.message())