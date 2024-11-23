from abc import ABC, abstractmethod
import json
import boto3
import logging

logger = logging.getLogger(__name__)

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
            logger.info("Poll request from S3 bucket")
            objects = list(self.__bucket.objects.limit(count=1))
            if not objects:
                logger.info("No items found in S3 bucket")
                return # Return None if no objects are found
            request_object = objects[0]
                
            json_string = request_object.get()["Body"].read().decode("utf-8")
            request = json.loads(json_string)
            logger.info(f"Found request with id {request['requestId']} in S3 bucket")
            
            request_object.delete()
            return request
        except:
            logger.warning("Failed to poll request from S3 bucket")