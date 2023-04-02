'''
Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''

# delete_old_snapshots_aurora
# This Lambda function will delete snapshots that have expired and match the regex set in the PATTERN environment variable. It will also look for a matching timestamp in the following format: YYYY-MM-DD-HH-mm
# Set PATTERN to a regex that matches your Aurora cluster identifiers (by default: <instance_name>-cluster)
import boto3
from datetime import datetime
import time
import os
import logging
import re
from snapshots_tool_utils import *

LOGLEVEL = os.getenv('LOG_LEVEL', 'ERROR').strip()
PATTERN = os.getenv('PATTERN', 'ALL_CLUSTERS')
RETENTION_DAYS = int(os.getenv('RETENTION_DAYS', '7'))
TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M'

if os.getenv('REGION_OVERRIDE', 'NO') != 'NO':
    REGION = os.getenv('REGION_OVERRIDE').strip()
else:
    REGION = os.getenv('AWS_DEFAULT_REGION')


logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())




def lambda_handler(event, context):
    pending_delete = 0
    client = boto3.client('rds', region_name=REGION)
    response = paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots')

    filtered_list = get_own_snapshots_source(PATTERN, response)

    for snapshot in filtered_list.keys():

        creation_date = get_timestamp(snapshot, filtered_list)

        if creation_date:

            difference = datetime.now() - creation_date

            days_difference = difference.total_seconds() / 3600 / 24

            logger.debug('%s created %s days ago' %
                         (snapshot, days_difference))

            # if we are past RETENTION_DAYS
            if days_difference > RETENTION_DAYS:

                # delete it
                logger.info('Deleting %s' % snapshot)

                try:
                    client.delete_db_cluster_snapshot(
                        DBClusterSnapshotIdentifier=snapshot)

                except Exception as e:
                    pending_delete += 1
                    logger.info(e)
                    logger.info('Could not delete %s ' % snapshot)

            else:
            # Not older than RETENTION_DAYS
                logger.debug('%s created less than %s days. Not deleting' % (snapshot, RETENTION_DAYS))

        else:
        # Did not have a timestamp
            logger.debug('Not deleting %s. Could not find a timestamp in the name' % snapshot)


    if pending_delete > 0:
        message = 'Snapshots pending delete: %s' % pending_delete
        logger.error(message)
        raise SnapshotToolException(message)


if __name__ == '__main__':
    lambda_handler(None, None)
