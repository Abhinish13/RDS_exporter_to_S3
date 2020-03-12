
# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from __future__ import print_function
import boto3
import botocore
import sys
from datetime import datetime
'''
Print the Usage of the Following Function explains the Neccessary parameter and type of parameters are required by the function
'''


def print_usage():
	print("Usage:\n<script_name>\n" +
        "\t--bucketname <S3 Buket Name>\n" +
        "\t--rdsinstancename <RDS Instance Name>\n" +
        "\t[--lognameprefix <Log Name Prefix>]\n" +
        "\t--region <Region>")


'''
Scanning the Paramter for Checking which flag or option is given by the user in command line
Necessary function for checking if any value is provided or not
'''


def parse_args(args):
	global config
	i = 1
	while i < len(args):
	arg = args[i]

		if arg == "--bucketname" and i + 1 < len(args):
		config['BucketName'] = args[i + 1]
			i += 1
		elif arg == "--rdsinstancename" and i + 1 < len(args):
		config['RDSInstanceName'] = args[i + 1]
			i += 1
		elif arg == "--lognameprefix" and i + 1 < len(args):
		config['LogNamePrefix'] = args[i + 1]
			i += 1
		elif arg == "--region" and i + 1 < len(args):
		config['Region'] = args[i + 1]
			i += 1
        elif arg == "--accesskey" and i + 1 < len(args):
        config['AccessKey'] = args[i + 1]
            i += 1
        elif arg == "--secretkey" and i + 1 < len(args):
		config['SecretKey'] = args[i + 1]
			i += 1
		else:
			print("ERROR: Invalid command line argument " + arg + "/ No value specified")
			print_usage()
			return False
		i += 1

	return True


'''
##########################################################################################################################################
												MAIN Function
##########################################################################################################################################
This is the main function which is handling the complete rds to s3 trasportation, this function uses boto3 module to connect to s3 and rds
Each parameter defination is provided properly for understanding.
##########################################################################################################################################

'''


def copy_logs_from_RDS_to_S3():

	global config

	# get settings from the config
	if {'BucketName', 'RDSInstanceName', 'Region', 'AccessKey', 'SecretKey'}.issubset(config):
	S3BucketName = config['BucketName']
		RDSInstanceName = config['RDSInstanceName']
		region = config['Region']
		if "LogNamePrefix" in config:
		logNamePrefix = config['LogNamePrefix']
		else:
		logNamePrefix = ""
		configFileName = RDSInstanceName + "/" + "backup_config"
        access = config['AccessKey']
        secret = config['SecretKey']
	else:
	print("ERROR: Values for the required field not specified")
		print_usage()
		return

	# initialize
	RDSclient = boto3.client('rds', region_name=region, aws_access_key_id=access,
                          aws_secret_access_key=secret)
	S3client = boto3.client('s3', region_name=region, aws_access_key_id=access,
                         aws_secret_access_key=secret)
	lastWrittenTime = 0
	lastWrittenThisRun = 0
	backupStartTime = datetime.now()

	# check if the S3 bucket exists and is accessible
	try:
	S3response = S3client.head_bucket(Bucket=S3BucketName)
	except botocore.exceptions.ClientError as e:
	error_code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
		if error_code == 404:
		print("Error: Bucket name provided not found")
			return
		else:
		print("Error: Unable to access bucket name, error: " +
		      e.response['Error']['Message'])
			return

    # get the config file, if the config isn't present this is the first run
	try:
	S3response = S3client.get_object(Bucket=S3BucketName, Key=configFileName)
		lastWrittenTime = int(S3response['Body'].read(S3response['ContentLength']))
		print("Found marker from last log download, retrieving log files with lastWritten time after %s" %
		      str(lastWrittenTime))
	except botocore.exceptions.ClientError as e:
	error_code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
		if error_code == 404:
		print("It appears this is the first log import, all files will be retrieved from RDS")
		else:
		print("Error: Unable to access config file, error: " +
		      e.response['Error']['Message'])
			return

	# copy the logs in batches to s3
	copiedFileCount = 0
	logMarker = ""
	moreLogsRemaining = True
	while moreLogsRemaining:
	dbLogs = RDSclient.describe_db_log_files(DBInstanceIdentifier=RDSInstanceName,
	                                         FilenameContains=logNamePrefix, FileLastWritten=lastWrittenTime, Marker=logMarker)
		if 'Marker' in dbLogs and dbLogs['Marker'] != "":
		logMarker = dbLogs['Marker']
		else:
		moreLogsRemaining = False

		# copy the logs in this batch
		for dbLog in dbLogs['DescribeDBLogFiles']:
		print("FileNumber: ", copiedFileCount + 1)
			print("Downloading log file: %s found and with LastWritten value of: %s " %
			      (dbLog['LogFileName'], dbLog['LastWritten']))
			if int(dbLog['LastWritten']) > lastWrittenThisRun:
			lastWrittenThisRun = int(dbLog['LastWritten'])

			# download the log file
			logFileData = ""
			try:
				logFile = RDSclient.download_db_log_file_portion(
				    DBInstanceIdentifier=RDSInstanceName, LogFileName=dbLog['LogFileName'], Marker='0')
				logFileData = logFile['LogFileData']
				while logFile['AdditionalDataPending']:
					logFile = RDSclient.download_db_log_file_portion(
					    DBInstanceIdentifier=RDSInstanceName, LogFileName=dbLog['LogFileName'], Marker=logFile['Marker'])
					logFileData += logFile['LogFileData']
			except Exception as e:
			print("File download failed: ", e)
				continue

			logFileDataCleaned = logFileData.encode(errors='ignore')
			logFileAsBytes = str(logFileDataCleaned).encode()

			# upload the log file to S3
			objectName = RDSInstanceName + "/" + "backup_" + \
				backupStartTime.isoformat() + "/" + dbLog['LogFileName']
			try:
				S3response = S3client.put_object(
				    Bucket=S3BucketName, Key=objectName, Body=logFileAsBytes)
				copiedFileCount += 1
			except botocore.exceptions.ClientError as e:
			print("Error writting object to S3 bucket, S3 ClientError: " +
			      e.response['Error']['Message'])
				return
			print("Uploaded log file %s to S3 bucket %s" % (objectName, S3BucketName))

	print("Copied ", copiedFileCount, "file(s) to s3")

	# Update the last written time in the config
	if lastWrittenThisRun > 0:
	try:
			S3response = S3client.put_object(
			    Bucket=S3BucketName, Key=configFileName, Body=str.encode(str(lastWrittenThisRun)))
		except botocore.exceptions.ClientError as e:
		print ("Error writting the config to S3 bucket, S3 ClientError: " + e.response['Error']['Message'])
			return
		print("Wrote new Last Written file to %s in Bucket %s" % (configFileName, S3BucketName))

	print("Log file export complete")

###### START OF SCRIPT ####


config = {}

# config = {'BucketName': "<bucket-name>", 'RDSInstanceName': "<instance-name>", 'LogNamePrefix': "", 'Region': "<>region-name"}

if(parse_args(sys.argv)):
	copy_logs_from_RDS_to_S3()
