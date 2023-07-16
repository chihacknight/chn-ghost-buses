import boto3
import sys

ACCESS_KEY = sys.argv[1]
SECRET_KEY = sys.argv[2]

client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

result = client.get_acl()
print("Printing acl:")
print(result)

print("Printing bucket objects:")
print(client.list_objects_v2(
    Bucket='chn-ghost-buses-public'
))

client.put_object(Body="test_file.txt", Bucket='chn-ghost-buses-public', Key=SECRET_KEY)
