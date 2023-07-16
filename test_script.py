import boto3
import sys

ACCESS_KEY = sys.argv[1]
SECRET_KEY = sys.argv[2]

client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

print(client.list_objects_v2(
    Bucket='chn-ghost-buses-public'
))

client.upload_file("test_file.txt", 'chn-ghost-buses-public', "test_file.txt")