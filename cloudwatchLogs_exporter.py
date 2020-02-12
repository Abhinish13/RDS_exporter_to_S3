# for Manupulating the AWS resources
import boto3
import collections
import sys
import getopt
import json
# For date time manupulation for deletionDate
from datetime import datetime, date, time, timedelta

# For Floor function call
import math
region = "us-east-2"



def main(argv):
   try:
      opts, args = getopt.getopt(argv,"hs:a:",["secret=","access="])
   except getopt.GetoptError:
      
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print('Invalid Secret Key and Access Key')
         sys.exit()
      elif opt in ("-s", "--secret"):
         secret = arg
      elif opt in ("-a", "--access"):
         access = arg
   exporter(access,secret)
#    print 'Input file is "', inputfile
#    print 'Output file is "', outputfile

def exporter(access,secret):
   # yesterday = datetime.combine(date.today() - timedelta(1), time())
    #today = datetime.combine(date.today(), time())
    #unix_start = datetime(1970, 1, 1)
    # Defination of N i.e. How many day the logs will be there in the Cloudwatch
    nDays = 0
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
    logGroupName = "/aws/rds/instance/perf-masterdb/audit"

    destinationS3 = "cloudwatchlogs-db-audit"
    destinationS3Prefix = "db_logs"
    client = boto3.client("logs", region_name=region,aws_access_key_id=access, 
                      aws_secret_access_key=secret)
    response = client.create_export_task(
        taskName="Export_CloudwatchLogs_{}".format(datetime.now()),
        logGroupName=logGroupName,
        fromTime = int((startOfDay).total_seconds() * 1000),
        to = int((endOfDay).total_seconds() * 1000),
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

def retention_days(n):
    retentionInDays = [
        1,
        3,
        5,
        7,
        14,
        30,
        60,
        90,
        120,
        150,
        180,
        365,
        400,
        545,
        731,
        1827,
        3653,
    ]
    for retention_day in retentionInDays:
        if n < retention_day:
            return retention_day


if __name__ == "__main__":
       main(sys.argv[1:])
