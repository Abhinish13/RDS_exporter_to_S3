# for Manupulating the AWS resources
import boto3
import collections
import sys
import getopt
import json
from cloudWatchLogs_exporter import exporter
# For date time manupulation for deletionDate
from datetime import datetime, date, time, timedelta


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hs:a:", ["secret=", "access="])
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
    exporter(access, secret)
#    print 'Input file is "', inputfile
#    print 'Output file is "', outputfile


if __name__ == "__main__":
    main(sys.argv[1:])
