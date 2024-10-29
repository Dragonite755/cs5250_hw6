import boto3
import time

REQUESTS_BUCKET = "usu-cs5250-ratatouille-requests"

def main():
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(REQUESTS_BUCKET)

    end_time = time.time() + 5 # TODO: Change runtime from 5 seconds
    while time.time() < end_time:
        # Attempt to retrieve request with smallest key
        requests = list(bucket.objects.limit(1))
        if len(requests) > 0: # Check that request exist
            request = requests[0]
            process_request(request)
            
        time.sleep(0.1)
        
def process_request(request):
    print(f"Processing {request}")
    
if __name__ == "__main__":
    main()