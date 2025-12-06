import boto3
import os

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