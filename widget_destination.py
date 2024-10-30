from abc import ABC, abstractmethod
from botocore.exceptions import ClientError
import boto3
import json

class WidgetDestination(ABC):
    unchangeable_attributes = ["widgetId", "owner"]
    
    @abstractmethod
    def create(self, request):
        pass
        
    @abstractmethod
    def delete(self, request):
        pass
        
    @abstractmethod
    def update(self, request):
        pass
        
class BucketDestination(WidgetDestination):
    def __init__(self, bucket_name):
        s3 = boto3.resource("s3")
        self.__bucket = s3.Bucket(bucket_name)
        
    def create(self, request):
        print(f"Creating {request["widgetId"]}\n")
        key = self.make_key(request)
        json_string=json.dumps(request)
        self.__bucket.put_object(Body=json_string, Key=key)
        
    def delete(self, request):
        print(f"Deleting {request["widgetId"]}\n")
        try:
            key = self.make_key(request)
            widget_object = self.__bucket.Object(key)
            widget_object.load()
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
        
    def update_widget(self, widget, update):
        updated_widget = dict(key)
        for key, value in widget.items():
            if key in self.unchangeable_attributes or key == "otherAttributes"
                updated_widget[key] = value
        
    def make_key(self, data):
        owner = data["owner"].replace(" ", "-").lower()
        widgetId = data["widgetId"]
        return f"widgets/{owner}/{widgetId}"