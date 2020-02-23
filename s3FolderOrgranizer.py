import boto3
import os
import pathlib
from datetime import datetime, date, time
from s3Buck_Poli_Editor import S3PolicyEditor


def s3FolderOrganizer(dateStamp):
    path = pathlib.Path().absolute()
    print(path)
    fileName = "info"
    filePath = os.path.join(path, fileName)
    # Start Date and End Date for info

    infoFile = open(filePath, "w+")
    infoFile.write(
        "This folder is Created by Jenkins\nThis folder contains the CloudWatch logs\nDate of Logs : {}".format(
            dateStamp.date()
        )
    )
    # Month name
    month_Name = dateStamp.strftime("%B")
    # BucketDetails
    folderName = str(month_Name)+"/" + str(dateStamp.date())
    keyValue = folderName + "/" + fileName
    s3_client = boto3.client("s3")
    with open(filePath) as file:
        object = file.read()
        s3_client.put_object(
            Body=object,
            Bucket="cloudwatchlogs-db-ind-master",
            Key=keyValue,
            ContentType="content/txt",
            ContentEncoding="utf-8",
            StorageClass="STANDARD",
        )
    S3PolicyEditor(folderName)
    return folderName

# arn: aws: s3: : : cloudwatchlogs-db-ind-master
