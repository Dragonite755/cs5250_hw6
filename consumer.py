import time
import json
import jsonschema
import argparse
import logging

import widget_source
import widget_destination

REGION = "us-east-1"

class Consumer:
    def __init__(self, source, destination):
        self.__source = source
        self.__destination = destination 
        
    def process_requests(self, timeout=15):
        end_time = time.time() + timeout
        while self.check_time(end_time):
            # Retrieve request as JSON dictionary
            request = self.__source.poll_request()
            if not self.process_request(request):
                time.sleep(0.1)
        
    def process_request(self, request):
        if not request:
            return False
            
        # Modify request before processing
        request["id"] = request["widgetId"]
        request_type = request["type"]
        del request["widgetId"]
        del request["type"]
        
        if request_type == "create":
            self.__destination.create(request)
        elif request_type == "delete":
            self.__destination.delete(request)
        elif request_type == "update":
            self.__destination.update(request)
        else:
            return False
        return True
        
    def check_time(self, end_time):
        return time.time() < end_time
    
def create_command_parser():
    parser = argparse.ArgumentParser(
        prog="Consumer",
        description="Creates widget requests")
    parser.add_argument("-rb", "--request-bucket", metavar="Request Bucket", required=True)
        
    # The destination may be either an S3 bucket or a DynamoDB table
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-wb", "--widget-bucket", metavar="Widget Bucket") # Bucket
    group.add_argument("-dwt", "--dynamo-table", metavar="Widget DynamoDB Table") # DynamoDB table
    return parser
    
def parse_source(args):
    if args.request_bucket:
        return widget_source.BucketSource(args.request_bucket)
        
def parse_destination(args):
    if args.widget_bucket:
        return widget_destination.BucketDestination(args.widget_bucket)
    elif args.dynamo_table:
        return widget_destination.DynamoDBDestination(args.dynamo_table, REGION)

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%H:%M:%S',
        handlers=[logging.FileHandler('consumer.log', encoding='utf-8',),
            logging.StreamHandler()])
    
    command_parser = create_command_parser()
    args = command_parser.parse_args()
    
    source = parse_source(args)
    destination = parse_destination(args)
    consumer = Consumer(source, destination)
    
    consumer.process_requests()