from abc import ABC, abstractmethod
import logging
import boto3
import json

logger = logging.getLogger(__name__)

class WidgetDestination(ABC):
    """
    Create widget from request data and store it in destination
    """
    @abstractmethod
    def create(self, request):
        pass
        
    """
    Delete widget from destination (if it exists)
    """
    @abstractmethod
    def delete(self, request):
        pass
        
    """
    Update widget in destination (if it exists)
    """
    @abstractmethod
    def update(self, request):
        pass
        
class BucketDestination(WidgetDestination):
    def __init__(self, bucket_name):
        s3 = boto3.resource("s3")
        self.__bucket = s3.Bucket(bucket_name)
        self.__unchangeable = {"id", "owner"} # Unchangeable attributes
        self.__logger = logging.getLogger(__name__)
        
    def create(self, request, request_id):
        logger.info(f"Process a create request for widget {request['id']} in request {request['requestId']}")
        
        request_id = request["requestId"]
        del request["requestId"]
        
        key = self.make_key(request)
        json_string=json.dumps(request)
        
        try:
            self.__bucket.put_object(Body=json_string, Key=key)
            logger.info(f"Put in S3 bucket a widget with key = {key}")
        except:
            logger.warning(f"Could not put in S3 bucket a widget with key = {key}")
        
    def delete(self, request):
        logger.info(f"Process a delete request for widget {request['id']} in request {request['requestId']}")
        
        request_id = request["requestId"]
        del request["requestId"]
    
        key = self.make_key(request)
        try:
            # Read object with given key
            widget_object = self.__bucket.Object(key)
            widget_object.load() # Will cause exception if object does not exist
            widget_object.delete() 
            logger.info(f"Delete from S3 bucket a widget with key = {key}")
        except:
            logger.warning(f"Widget with key = {key} does not exist in S3 bucket")
        
    def update(self, request):
        logger.info(f"Process an update request for widget {request['id']} in request {request['requestId']}")
        
        request_id = request["requestId"]
        del request["requestId"]
        
        key = self.make_key(request)
        try:
            widget_object = self.__bucket.Object(key)
            widget_object.load() # Will cause exception if object does not exist
            old_json_string = widget_object.get()["Body"].read().decode("utf-8")
            old_widget = json.loads(old_json_string)
            updated_widget = self.update_widget(old_widget, request)
            json_string = json.dumps(updated_widget)
            widget_object.put(Body=json_string)
            logger.info(f"Update in S3 bucket a widget with key = {key}")
        except:
            logger.warning(f"Widget with key = {key} does not exist in S3 bucket")
        
    """
    Update the fields in widget with the fields from update
    """
    def update_attributes(self, widget, data):
        for attribute, value in data.items():
            if attribute == "otherAttributes":
                for otherData in data["otherAttributes"]:
                    for otherAttribute, otherValue in otherData.items():
                        if otherAttribute in widget:
                            if otherValue != "":
                                widget[otherAttribute] = otherValue
                            else:
                                del widget[otherAttribute]
            elif attribute not in self.__unchangeable:
                if value != "":
                    widget[attribute] = value
                else:
                    widget[attribute] = None
                    
    """
    Create key from request data (before it is cleaned)
    """
    def make_key(self, data):
        owner = data["owner"].replace(" ", "-").lower()
        widgetId = data["id"]
        return f"widgets/{owner}/{widgetId}"
        
class DynamoDBDestination(WidgetDestination):
    def __init__(self, table_name, region):
        dynamodb = boto3.resource("dynamodb", region_name=region)
        self.__table = dynamodb.Table(table_name)
        
    def create(self, request):
        logger.info(f"Process a create request for widget {request['id']} in request {request['requestId']}")
        
        request_id = request["requestId"]
        del request["requestId"]
        
        # Place all attributes in otherAttributes into their own attributes
        if "otherAttributes" in request:
            for data in request["otherAttributes"]:
                name = data["name"]
                value = data["value"]
                request[name] = value
            del request["otherAttributes"]
        
        key = request["id"]
        try:
            self.__table.put_item(Item=request)
            logger.info(f"Put in DynamoDB Table a widget with key = {key}")
        except:
            logger.warning(f"Could not put in DynamoDB Table a widget with key = {key}")
        
    def delete(self, request):
        logger.info(f"Process a delete request for widget {request['id']} in request {request['requestId']}")
        
        request_id = request["requestId"]
        del request["requestId"]
        
        key = request["id"]
        response = self.__table.get_item(Key=key)
        try:
            if 'Item' in response:
                self.__table
            else:
                logger.warning(f"Widget with key = {key} does not exist in DynamoDB Table")
        except:
            logger.warning(f"Could not delete widget with key = {key}")
        
    def update(self, request):
        logger.info(f"Process an update request for widget {request['id']} in request {request['requestId']}")
    
        request_id = request["requestId"]
        del request["requestId"]
        
        
        