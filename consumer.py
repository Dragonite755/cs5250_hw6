import boto3
import time
import json
import jsonschema
import argparse

REQUESTS_BUCKET = "usu-cs5250-ratatouille-requests"
JSON_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "type": {
            "type": "string",
            "pattern": "create|delete|update"
        },
        "requestId": {
            "type": "string"
        },
        "widgetId": {
            "type": "string"
        },
        "owner": {
            "type": "string",
            "pattern": "[A-Za-z ]+"
        },
            "label": {
            "type": "string"
        },
        "description": {
            "type": "string"
        },
        "otherAttributes": {
            "type": "array",
            "items": [
                {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string"
                        },
                        "value": {
                            "type": "string"
                        }
                    },
                    "required": [
                        "name",
                        "value"
                    ]
                }
            ]
        }
    },
    "required": [
        "type",
        "requestId",
        "widgetId",
        "owner"
    ]
}

class Consumer:
    def __init__(self, source, destination):
        pass
        
    
def create_command_parser():
    parser = argparse.ArgumentParser(
        prog="Consumer",
        description="Creates widget requests")
    parser.add_argument("-rb", "--request-bucket", metavar="Request Bucket", required=True)
        
    # The destination may be either an S3 bucket or a DynamoDB table
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-wb", "--widget-bucket", metavar="Widget Bucket") # Bucket
    group.add_argument("-dwt", metavar="Widget DynamoDB Table") # DynamoDB table
    return parser
        
if __name__ == "__main__":
    command_parser = create_command_parser()
    args = command_parser.parse_args()
    print(args)
    print(args.request_bucket)