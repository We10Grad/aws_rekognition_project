import boto3
import os
from datetime import datetime

def upload_to_s3(local_file_path, s3_key):
    """Upload a file to S3"""
    bucket_name = os.environ.get('S3_BUCKET')
    
    s3_client = boto3.client('s3')
    s3_client.upload_file(local_file_path, bucket_name, s3_key)
    print(f"Uploaded {local_file_path} to S3")

def analyze_image(image_key):
    """Analyze an image in S3 using Rekognition"""
    bucket_name = os.environ.get('S3_BUCKET')
    
    rekognition = boto3.client('rekognition')
    
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
    
    labels = []
    for label in response['Labels']:
        labels.append({
            'Name': label['Name'],
            'Confidence': round(label['Confidence'], 2)
        })
    
    return labels

def write_results_to_dynamodb(filename, labels):
    """Write analysis results to DynamoDB"""
    table_name = os.environ.get('DYNAMODB_TABLE')
    branch = os.environ.get('GITHUB_REF_NAME')
    
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    item = {
        'filename': filename,
        'labels': labels,
        'timestamp': timestamp,
        'branch': branch
    }
    
    table.put_item(Item=item)
    print(f"Wrote results to DynamoDB table {table_name}")

def main():
    """Main function to process all images"""
    images_folder = 'images'
    
    # Get all .jpg and .png files
    all_files = os.listdir(images_folder)
    image_files = []
    
    for file in all_files:
        if file.endswith('.jpg') or file.endswith('.png'):
            image_files.append(file)
    
    print(f"Found {len(image_files)} image(s) to process")
    
    # Process each image
    for image_file in image_files:
        print(f"\nProcessing {image_file}...")
        
        # Step 1: Upload to S3
        local_path = os.path.join(images_folder, image_file)
        s3_key = f"rekognition-input/{image_file}"
        upload_to_s3(local_path, s3_key)
        
        # Step 2: Analyze with Rekognition
        labels = analyze_image(s3_key)
        print(f"Found {len(labels)} labels")
        
        # Step 3: Write to DynamoDB
        write_results_to_dynamodb(s3_key, labels)
        print(f"✓ Completed {image_file}")

if __name__ == "__main__":
    main()