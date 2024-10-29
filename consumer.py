import boto3
import time
import json
import jsonschema

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

def main():
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(REQUESTS_BUCKET)

    end_time = time.time() + 1 # TODO: Change runtime from 1 seconds
    while time.time() < end_time:
        # Attempt to retrieve request with smallest key
        for request in bucket.objects.limit(1):
            # Load request body before processing
            parsed_request = parse_request_to_json(request.get()['Body'].read().decode("utf-8"))
            process_request(parsed_request)
            
        time.sleep(0.1)
        
# Process request (dict)
def process_request(request):
    print(f"Processing {request.key}:")
    
    print()
    
def parse_request_to_json(request):
    try:
        # Parse data and validate against schema
        data = json.loads()
        jsonschema.validate(instance=data, schema=JSON_SCHEMA)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e.message}")
    except jsonschema.ValidationError as e:
        print(f"Error validating Schema: {e.message}")
    return data
    
if __name__ == "__main__":
    main()