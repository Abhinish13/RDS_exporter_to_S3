
import boto3
import collections
import sys
import getopt
import json
from retention_policy_setter import retention_days
from s3FolderOrgranizer import s3FolderOrganizer
# For date time manupulation for deletionDate
from datetime import datetime, date, time, timedelta
# For Floor function call
import math
region = "ap-south-1"


def exporter(access, secret):
    # yesterday = datetime.combine(date.today() - timedelta(1), time())
    #today = datetime.combine(date.today(), time())
    #unix_start = datetime(1970, 1, 1)
    # Defination of N i.e. How many day the logs will be there in the Cloudwatch
    nDays = 14
    # Declaration of the Deletion date which will be used to delete the cloudwatch logs
    deletionDate = datetime.now() - timedelta(days=nDays)
    # Print the deletionDate for confirmation of date
    print(deletionDate)
    # For deletionDate : replacing the time for deleting the cloudwatch
    # Start of Day: Begining of the day i.e 00:00:00:0000 Time at DeletionDate
    unix_start = datetime(1970, 1, 1)
    startOfDay = datetime.combine(date.today(), time()).replace(
        hour=00, minute=00, second=00, microsecond=000000) - unix_start
    # End of Dat : Ending of the day i.e. 23:59:59:9999999 Time at DeletionDate
    endOfDay = datetime.combine(date.today(), time()).replace(
        hour=23, minute=59, second=59, microsecond=999999) - unix_start
    # Log group of AWS resource which will be deleted is stored to variable group_name
    # It a list to delete multiple Logs group
    logGroupName = "/aws/rds/instance/ind-master-db/audit"
    # Call of function
    s3_client = boto3.client("s3",  region_name=region, aws_access_key_id=access,
                             aws_secret_access_key=secret)
    destinationS3 = "cloudwatchlogs-db-ind-master"
    destinationS3Prefix = s3FolderOrganizer(deletionDate, s3_client)
    client = boto3.client("logs", region_name=region, aws_access_key_id=access,
                          aws_secret_access_key=secret)

    response = client.create_export_task(
        taskName="Export_CloudwatchLogs_{}".format(datetime.now()),
        logGroupName=logGroupName,
        fromTime=int((startOfDay).total_seconds() * 1000),
        to=int((endOfDay).total_seconds() * 1000),
        destination=destinationS3,
        destinationPrefix=destinationS3Prefix,
    )
    print(
        "Response from export task at {} :\n{}".format(
            datetime.now().isoformat(), response
        )
    )
    response = client.put_retention_policy(
        logGroupName=logGroupName,
        # Function call for RetentionInDays(1) nDays = 1
        retentionInDays=retention_days(nDays),
    )
