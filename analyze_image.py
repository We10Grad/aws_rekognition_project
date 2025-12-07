import boto3
import os
from datetime import datetime

def analyze_image(image_key):
    # Get the S3 bucket name from environment variable
    bucket_name = os.environ.get('S3_BUCKET')
    
    # Create the Rekognition client
    rekognition = boto3.client('rekognition')
    
    # Call Rekognition to detect labels
    response = rekognition.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': image_key
            }
        },
        MaxLabels=10,
        MinConfidence=75
    )
    
    # Clean up the response - extract just Name and Confidence
    labels = []
    for label in response['Labels']:
        labels.append({
            'Name': label['Name'],
            'Confidence': round(label['Confidence'], 2)
        })
    
    return labels

def write_results_to_dynamodb(filename, labels):
    # 1. Get table name from environment variable
    table_name = os.environ.get('DYNAMODB_TABLE')  
    
    # 2. Get branch name from environment variable
    branch = os.environ.get('GITHUB_REF_NAME') 
    
    # 3. Generate timestamp
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # 4. Create DynamoDB resource
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    # 5. Build the item dictionary
    item = {
        'filename': filename,
        'labels': labels,
        'timestamp': timestamp,
        'branch': branch
    }
    
    # 6. Write to DynamoDB
    table.put_item(Item=item)