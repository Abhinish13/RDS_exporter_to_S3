import boto3
import collections
import sys, getopt
from datetime import datetime, date, time, timedelta

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
    yesterday = datetime.combine(date.today() - timedelta(1), time())
    today = datetime.combine(date.today(), time())
    unix_start = datetime(1970, 1, 1)
    client = boto3.client("logs", region_name=region,aws_access_key_id=access, 
                      aws_secret_access_key=secret)
    response = client.create_export_task(
        taskName="Export_CloudwatchLogs_{}".format(today),
        logGroupName="/aws/rds/instance/perf-masterdb/audit",
        fromTime=int((yesterday - unix_start).total_seconds() * 1000),
        to=int((today - unix_start).total_seconds() * 1000),
        destination="cloudwatchlogs-db-audit",
        destinationPrefix="db_logs",
    )
    print(
        "Response from export task at {} :\n{}".format(
            datetime.now().isoformat(), response
        )
    )


if __name__ == "__main__":
       main(sys.argv[1:])
