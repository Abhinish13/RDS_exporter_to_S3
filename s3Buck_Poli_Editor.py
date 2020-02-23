import json
import boto3


def S3PolicyEditor(folderName, s3_client):
    BUCKET_NAME = "cloudwatchlogs-db-ind-master"
    # Retrieve the policy of the specified bucket
    #s3 = boto3.client("s3")

    # Create a bucket policy
    Resource = "arn:aws:s3:::cloudwatchlogs-db-ind-master/{0}/*".format(
        folderName)
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": "s3:GetBucketAcl",
                "Effect": "Allow",
                "Resource": "arn:aws:s3:::cloudwatchlogs-db-ind-master",
                "Principal": {"Service": "logs.ap-south-1.amazonaws.com"},
            },
            {
                "Action": "s3:PutObject",
                "Effect": "Allow",
                "Resource": Resource,
                "Condition": {
                    "StringEquals": {"s3:x-amz-acl": "bucket-owner-full-control"}
                },
                "Principal": {"Service": "logs.ap-south-1.amazonaws.com"},
            },
        ],
    }

    # Convert the policy from JSON dict to string
    bucket_policy = json.dumps(bucket_policy)

    s3_client.put_bucket_policy(Bucket=BUCKET_NAME, Policy=bucket_policy)

    result = s3_client.get_bucket_policy(Bucket=BUCKET_NAME)
    print("S3 Bucket Policy :\n {}".format(
        json.dumps(result["Policy"], indent=4, sort_keys=True)))

# Set the new policy
# s3 = boto3.client("s3")
# s3.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)

# Delete a bucket's policy
# s3 = boto3.client("s3")
# s3.delete_bucket_policy(Bucket=BUCKET_NAME)
