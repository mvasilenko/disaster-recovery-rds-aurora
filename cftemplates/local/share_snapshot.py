'''
Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''

# share_snapshots_aurora
# This Lambda function shares snapshots created by aurora_take_snapshot with the account set in the environment variable DEST_ACCOUNT
# It will only share snapshots tagged with shareAndCopy and a value of YES
import boto3
from datetime import datetime
import time
import os
import logging
import re
from snapshots_tool_utils import *
import sys


# Initialize from environment variable
LOGLEVEL = 'INFO'
DEST_ACCOUNTID = ''
REGION = 'us-east-1'
BACKUP_KMS = ''


SNAPSHOTS_PATTERN = '^stool.*$'
BACKUP_NAME_PREFIX = 'stool'

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())



def lambda_handler(event, context):
    pending_snapshots = 0
    now = datetime.now()
    client = boto3.client('rds', region_name=REGION)
    response = paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots', SnapshotType='manual')
    filtered = get_own_snapshots_share(SNAPSHOTS_PATTERN, response)

    # Search all snapshots for the correct tag
    for snapshot_identifier,snapshot_object in filtered.items():
        snapshot_arn = snapshot_object['Arn']

        response_tags = client.list_tags_for_resource(
            ResourceName=snapshot_arn)

        if snapshot_object['Status'].lower() == 'available' and search_tag_share(response_tags):
            snapshot_info = client.describe_db_cluster_snapshots(
                DBClusterSnapshotIdentifier=snapshot_arn
            )
            timestamp_format = now.strftime('%Y-%m-%d-%H-%M')

            if BACKUP_NAME_PREFIX != 'NONE' and BACKUP_NAME_PREFIX != '':
                targetSnapshot = '%s-%s-%s' % (
                    BACKUP_NAME_PREFIX, snapshot_info['DBClusterSnapshots'][0]['DBClusterIdentifier'],timestamp_format
                )
            else:
                targetSnapshot = snapshot_info['DBClusterSnapshots'][0]['DBClusterIdentifier'] + '-' + timestamp_format

            print("Check for KMS encryption on db: {}".format(snapshot_identifier))
            kms = False
            if snapshot_info['DBClusterSnapshots'][0]['StorageEncrypted'] == True:
                try:
                    # Evaluate if kms in snapshot is default kms or custom kms
                    kms = get_kms_type(snapshot_info['DBClusterSnapshots'][0]['KmsKeyId'],REGION)
                except Exception as e:
                    print('Exception get_kms_type {}: {}'.format(snapshot_identifier, e))
                    print('Its a copied snapshot with a key from DEST account')
            else:
                print("Snapshot: {} is not encrypted".format(snapshot_identifier))

            print('Checking Snapshot: {}'.format(snapshot_identifier))
            if kms is True and BACKUP_KMS != '':
                try:
                    print('Running copy: {}'.format(snapshot_identifier))
                    copy_status = client.copy_db_cluster_snapshot(
                    SourceDBClusterSnapshotIdentifier=snapshot_arn,
                    TargetDBClusterSnapshotIdentifier=targetSnapshot,
                    KmsKeyId=BACKUP_KMS,
                    CopyTags=True
                )
                    pass
                except Exception as e:
                    print('Exception copy {}: {}'.format(snapshot_arn, e))
                    pending_snapshots += 1
                    pass
                else:
                    # set source snapshot with def kms to not copy
                    modify_status = client.add_tags_to_resource(
                    ResourceName=snapshot_arn,
                    Tags=[
                        {
                            'Key': 'shareAndCopy',
                            'Value': 'No'
                        }
                        ]
                        )
                    
            try:
                # Share snapshot with dest_account
                if kms is True:
                    snapshot_to_copy = targetSnapshot
                else:
                    snapshot_to_copy = snapshot_arn


                response_modify = client.modify_db_cluster_snapshot_attribute(
                DBClusterSnapshotIdentifier=snapshot_to_copy,
                AttributeName='restore',
                ValuesToAdd=[
                    DEST_ACCOUNTID
                ]
                )
                print('Sharing: {}'.format(snapshot_to_copy))

                # after sharing, disable share and copy on the shared snapshot
                modify_status = client.add_tags_to_resource(
                ResourceName=snapshot_to_copy,
                Tags=[
                    {
                        'Key': 'shareAndCopy',
                        'Value': 'No'
                    }
                    ]
                    )

            except Exception as e:
                print('Exception sharing {}: {}'.format(snapshot_to_copy, e))
                pending_snapshots += 1
                

    if pending_snapshots > 0:
        log_message = 'Could not share all snapshots. Pending: %s' % pending_snapshots
        print(log_message)
        raise SnapshotToolException(log_message)


if __name__ == '__main__':
    lambda_handler(None, None)
