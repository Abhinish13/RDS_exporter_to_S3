import boto3
import collections
from datetime import datetime, date, time, timedelta

region = "us-east-2"


def exporter():
    yesterday = datetime.combine(date.today() - timedelta(1), time())
    today = datetime.combine(date.today(), time())
    unix_start = datetime(1970, 1, 1)
    client = boto3.client("logs", region_name=region)
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


exporter()
