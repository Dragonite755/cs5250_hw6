from abc import ABC, abstractmethod
import boto3
import json

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
        self.__unchangeable = {"widgetId", "owner"} # Unchangeable attributes
        
    def create(self, request):
        print(f"Creating {request["widgetId"]}\n")
        key = self.make_key(request)
        json_string=json.dumps(request)
        self.__bucket.put_object(Body=json_string, Key=key)
        
    def delete(self, request):
        print(f"Deleting {request["widgetId"]}\n")
        key = self.make_key(request)
        try:
            # Read object with given key
            widget_object = self.__bucket.Object(key)
            widget_object.load() # Will cause exception if object does not exist
            widget_object.delete()
        except ClientError as e:
            print("Object does not exist")
        
    def update(self, request):
        print(f"Updating {request["widgetId"]}\n")
        try:
            key = self.make_key(request)
            widget_object = self.__bucket.Object(key)
            old_json_string = widget_object.get()["Body"].read().decode("utf-8")
            old_widget = json.loads(old_json_string)
            updated_widget = self.update_widget(old_widget, request)
            json_string = json.dumps(updated_widget)
            widget_object.put(Body=json_string)
        except ClientError as e:
            print("Object does not exist")
        
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
    Create key from request data
    """
    def make_key(self, data):
        owner = data["owner"].replace(" ", "-").lower()
        widgetId = data["widgetId"]
        return f"widgets/{owner}/{widgetId}"
        
class DynamoDBDestination(WidgetDestination):
    def __init__(self, table_name):
        dynamodb = boto3.resource("dynamodb")
        self.__table = dynamodb.Table(table_name)
        
    def create(self, request):
        createWidgetData(request)
        json_string=json.dumps(request)
        self.__table.put_item(Item=json_string)
        
    def delete(self, request):
        id = request["widgetId"]
        try:
            self.__table.delete_item(Key=key)
        except:
            print("WARNING Widget with id "{key}" does not exist")
        
    def update(self, request):
        createWidgetData(request)
        
    def createWidgetData(self, request):
        if "otherAttributes" in request:
            for data in request["otherAttributes"]:
                for attribute, value in data.items():
                    request[attribute] = value
        request["id"] = request["widgetId"]
        del request["widgetId"]