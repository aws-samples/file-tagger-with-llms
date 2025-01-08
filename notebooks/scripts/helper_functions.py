"""
Copyright 2025 Amazon.com, Inc. or its affiliates.  All Rights Reserved.
SPDX-License-Identifier: MIT-0
"""
import boto3
from botocore.exceptions import ClientError
import datetime
import json
import logging
import os
import sagemaker
import time

# Create the logger
DEFAULT_LOG_LEVEL = logging.NOTSET
DEFAULT_LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
log_level = os.environ.get('LOG_LEVEL')
match log_level:
    case '10':
        log_level = logging.DEBUG
    case '20':
        log_level = logging.INFO
    case '30':
        log_level = logging.WARNING
    case '40':
        log_level = logging.ERROR
    case '50':
        log_level = logging.CRITICAL
    case _:
        log_level = DEFAULT_LOG_LEVEL
log_format = os.environ.get('LOG_FORMAT')
if log_format is None:
    log_format = DEFAULT_LOG_FORMAT
elif len(log_format) == 0:
    log_format = DEFAULT_LOG_FORMAT
# Set the basic config for the logger
logging.basicConfig(level=log_level, format=log_format)


def substring_after(source_string, substring) -> str:
    """
    Function to get the substring after a specified string within a source string.

    Parameters:
    source_string (str): The source string
    substring (str): The substring

    Returns:
    str: The string after the substring
    """
    source_string_parts = source_string.rsplit(substring, 1)
    return source_string_parts[1]


def list_s3_files(s3_bucket_name, s3_key_prefix) -> list[str]:
    """
    Function to list all the files in the specified S3 bucket.

    If the S3 key prefix is empty, then, all the files in the bucket will be listed.

    Parameters:
    s3_bucket_name (str): The S3 bucket name
    s3_key_prefix (str): The name of the file which will also serve as the S3 key prefix.

    Returns:
    list[str]: The list of file (S3 object) names with prefixes, if any
    """
    return sagemaker.Session().list_s3_files(bucket=s3_bucket_name, key_prefix=s3_key_prefix)


def download_from_s3(dir_name, s3_bucket_name, s3_key_prefix) -> list[str]:
    """
    Function to download the specified files/directory from a S3 bucket to the specified local directory.
    
    If the S3 key prefix is empty, then, all the files in the bucket will be downloaded with the directory structure as they exist in the bucket.

    Parameters:
    dir_name (str): The name of the local directory to where the file should be downloaded
    s3_bucket_name (str): The S3 bucket name
    s3_key_prefix (str): The name of the file which will also serve as the S3 key prefix.

    Returns:
    list[str]: The list of local paths of the downloaded files
    """
    downloaded_file_paths = sagemaker.Session().download_data(path='{}/'.format(dir_name),
                                                              bucket=s3_bucket_name,
                                                              key_prefix=s3_key_prefix)
    logging.debug("Downloaded {} file(s) from '{}' to '{}'.".format(len(downloaded_file_paths), s3_bucket_name, dir_name))
    return downloaded_file_paths


def delete_from_s3(s3_client, s3_bucket_name, s3_key) -> None:
    """
    Function to delete the specified object from the specified S3 bucket.

    Parameters:
    s3_client (S3.Client): The boto3 client for S3
    s3_bucket_name (str): The S3 bucket name
    s3_key (str): The key to the object on S3.

    Returns:
    None
    """
    # Get the AWS Region name from the boto3 client
    region_name = s3_client.meta.region_name
    # Delete the S3 object
    logging.info('Deleting object "{}" from S3 bucket "{}" in region "{}"...'.format(s3_key,
                                                                                     s3_bucket_name,
                                                                                     region_name))
    s3_client.delete_object(
        Bucket=s3_bucket_name,
        Key=s3_key,
    )
    logging.info('Completed deleting object.')


def delete_local_file(file_full_path) -> None:
    """
    Function to delete the specified file from a local directory.

    Parameters:
    file_full_path (str): The full path to the local file

    Returns:
    None
    """
    # Delete the file
    logging.info('Deleting local file "{}"...'.format(file_full_path))
    os.remove(file_full_path)
    logging.info('Completed deleting local file.')


def check_and_create_bda_blueprint(bda_client, blueprint_name, blueprint_type, blueprint_desc, doc_class) -> str:
    """
    Function to check and create the specified blueprint in Amazon Bedrock Data Automation (BDA).

    Parameters:
    bda_client (DynamoDB.Client): The boto3 client for BDA
    blueprint_name (str): The name of the blueprint
    blueprint_type (str): The type of the blueprint
    blueprint_desc (str): The description of the blueprint
    doc_class (str): The class of the document associated with this blueprint

    Returns:
    str: The ARN of the blueprint
    """
    # Get the AWS Region name from the boto3 client
    region_name = bda_client.meta.region_name
    # Check if the blueprint exists
    blueprints = bda_client.list_blueprints()["blueprints"]
    for blueprint in blueprints:
        if blueprint['blueprintName'] == blueprint_name:
            logging.info('BDA blueprint "{}" exists in region "{}" and is ready to use.'.format(blueprint_name,
                                                                                                 region_name))
            logging.info('BDA blueprint creation ignored.')
            # Return the blueprint ARN
            return blueprint['blueprintArn']
    # Process the blueprint not exists scenario
    logging.info('BDA blueprint "{}" does not exist in region "{}".'.format(blueprint_name, region_name))
    # Create the blueprint if it does not exist
    logging.info('Creating BDA blueprint "{}" in region "{}"...'.format(blueprint_name, region_name))
    create_blueprint_response = bda_client.create_blueprint(
        blueprintName=blueprint_name,
        type=blueprint_type,
        blueprintStage='LIVE',
        schema=json.dumps({
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'description': blueprint_desc,
            'documentClass': doc_class,
            'type': 'object',
            'properties': {
                'pii_indicator': {
                    'type': 'boolean',
                    'inferenceType': 'extractive',
                    'description': 'Does this document contain PII information or not? If it contains PII, then set this to true. If it does not contain PII, then set this to false. If you cannot determine it to be either true or false, then, set this value to None.'
                },
                'pii_explanation': {
                    'type': 'string',
                    'inferenceType': 'extractive',
                    'description': 'Explanation for the presence or absence of PII information.'
                },
            }
        })
    )
    # Return the blueprint ARN
    return create_blueprint_response['blueprint']['blueprintArn']


def check_and_create_bda_blueprint_version(bda_client, blueprint_arn, blueprint_version_arn) -> str:
    """
    Function to check and create a version of the specified blueprint in Amazon Bedrock Data Automation (BDA).

    Parameters:
    bda_client (DynamoDB.Client): The boto3 client for BDA
    blueprint_arn (str): The ARN of the blueprint
    blueprint_version_arn (str): The ARN of the version of the blueprint, if it exists

    Returns:
    str: The ARN of the blueprint version
    """
    # Get the AWS Region name from the boto3 client
    region_name = bda_client.meta.region_name
    # Check if the blueprint exists
    try:
        # Check if the version of the blueprint is specified or not; process accordingly
        if len(blueprint_version_arn) == 0:
            bda_client.get_blueprint(blueprintArn=blueprint_arn, blueprintStage='LIVE')
            logging.info('BDA blueprint with ARN "{}" exists in region "{}" and is ready to use.'.format(blueprint_arn,
                                                                                                         region_name))
        else:
            get_blueprint_response = bda_client.get_blueprint(blueprintArn=blueprint_arn,
                                                              blueprintVersion=blueprint_version_arn,
                                                              blueprintStage='LIVE')
            logging.info('BDA blueprint with ARN "{}" exists in region "{}" and is ready to use.'.format(blueprint_arn,
                                                                                                         region_name))
            # Check if the blueprint version exists
            if 'blueprintVersion' in get_blueprint_response['blueprint']:
                # Blueprint version exists; return this version
                retrieved_blueprint_version_arn = get_blueprint_response['blueprint']['blueprintVersion']
                logging.info('Version "{}" exists in BDA blueprint.'.format(retrieved_blueprint_version_arn))
                logging.info('BDA blueprint version creation ignored.')
                return retrieved_blueprint_version_arn
            else:
                logging.info('No versions exist in BDA blueprint.')
    except ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            logging.info('BDA blueprint with ARN "{}" does not exist in region "{}".'.format(blueprint_arn,
                                                                                             region_name))
        else:
            logging.error(error)
        return ''
    # Create the blueprint version
    logging.info('Creating a version of the BDA blueprint with ARN "{}" in region "{}"...'.format(blueprint_arn,
                                                                                                  region_name))
    create_blueprint_version_response = bda_client.create_blueprint_version(blueprintArn=blueprint_arn)
    blueprint_version_arn = create_blueprint_version_response['blueprint']['blueprintVersion']
    logging.info('Completed creating a version of the BDA blueprint. Version ARN is "{}"'.format(blueprint_version_arn))
    # Return the blueprint version ARN
    return blueprint_version_arn


def check_and_create_bda_project(bda_client, project_name, document_blueprint_arn, document_blueprint_version_arn,
                                 image_blueprint_arn, image_blueprint_version_arn) -> str:
    """
    Function to check and create the specified project in Amazon Bedrock Data Automation (BDA).

    Parameters:
    bda_client (DynamoDB.Client): The boto3 client for BDA
    project_name (str): The name of the project
    document_blueprint_arn (str): The ARN of the document blueprint
    document_blueprint_version_arn (str): The ARN of the document blueprint version
    image_blueprint_arn (str): The ARN of the image blueprint
    image_blueprint_version_arn (str): The ARN of the image blueprint version

    Returns:
    project_arn (str): The ARN of the project
    """
    # Get the AWS Region name from the boto3 client
    region_name = bda_client.meta.region_name
    # Check if the project exists
    projects = bda_client.list_data_automation_projects()["projects"]
    for project in projects:
        if project['projectName'] == project_name:
            logging.info('BDA project "{}" exists in region "{}" and is ready to use.'.format(project_name,
                                                                                               region_name))
            logging.info('BDA project creation ignored.')
            # Return the project ARN
            return project['projectArn']
    # Process the project not exists scenario
        logging.info('BDA project "{}" does not exist in region "{}".'.format(project_name, region_name))
        # Create the project if it does not exist
        logging.info('Creating BDA project "{}" in region "{}"...'.format(project_name, region_name))
        create_project_response = bda_client.create_data_automation_project(
            projectName=project_name,
            projectDescription='BDA project for {}'.format(project_name),
            projectStage='LIVE',
            standardOutputConfiguration={
                'document': {
                    'extraction': {
                        'granularity': {
                            'types': [
                                'DOCUMENT',
                            ]
                        },
                        'boundingBox': {
                            'state': 'DISABLED'
                        }
                    },
                    'generativeField': {
                        'state': 'ENABLED'
                    },
                    'outputFormat': {
                        'textFormat': {
                            'types': [
                                'PLAIN_TEXT',
                            ]
                        },
                        'additionalFileFormat': {
                            'state': 'DISABLED'
                        }
                    }
                },
                'image': {
                    'extraction': {
                        'category': {
                            'state': 'DISABLED',
                        },
                        'boundingBox': {
                            'state': 'DISABLED'
                        }
                    },
                    'generativeField': {
                        'state': 'ENABLED',
                        'types': [
                            'IMAGE_SUMMARY',
                        ]
                    }
                },
                'video': {
                    'extraction': {
                        'category': {
                            'state': 'DISABLED'
                        },
                        'boundingBox': {
                            'state': 'DISABLED'
                        }
                    },
                    'generativeField': {
                        'state': 'ENABLED',
                        'types': [
                            'VIDEO_SUMMARY',
                        ]
                    }
                },
                'audio': {
                    'extraction': {
                        'category': {
                            'state': 'DISABLED'
                        }
                    },
                    'generativeField': {
                        'state': 'ENABLED',
                        'types': [
                            'AUDIO_SUMMARY',
                        ]
                    }
                }
            },
            customOutputConfiguration={
                'blueprints': [
                    {
                        'blueprintArn': document_blueprint_arn,
                        'blueprintVersion': document_blueprint_version_arn,
                        'blueprintStage': 'LIVE'
                    },
                    {
                        'blueprintArn': image_blueprint_arn,
                        'blueprintVersion': image_blueprint_version_arn,
                        'blueprintStage': 'LIVE'
                    },
                ]
            },
        )
        create_project_status = create_project_response['status']
        if create_project_status == 'COMPLETED':
            logging.info('BDA project created successfully and is ready to use.')
        elif create_project_status == 'FAILED':
            logging.error('BDA project creation failed.')
        else:
            counter = 1
            while True:
                logging.info('Sleeping for 5 seconds to re-check BDA project creation status...')
                time.sleep(5)
                projects = bda_client.list_data_automation_projects()["projects"]
                for project in projects:
                    if project['projectName'] == project_name:
                        logging.info('BDA project created successfully and is ready to use.')
                        # Return the project ARN
                        return project['projectArn']
                counter += 1
                if counter == 60:
                    logging.info('BDA project creation status unknown. Status check exited.')
                    return ''


def check_and_create_metadata_table(ddb_client, table_name) -> None:
    """
    Function to check and create the specified metadata table in Amazon DynamoDB to store tags related to specific files.

    Parameters:
    ddb_client (DynamoDB.Client): The boto3 client for DynamoDB
    table_name (str): The name of the table

    Returns:
    None
    """
    # Get the AWS Region name from the boto3 client
    region_name = ddb_client.meta.region_name
    # Set the table exists flag
    table_exists = False
    # Check if the table exists and also it's status
    try:
        describe_table_response = ddb_client.describe_table(
            TableName=table_name
        )
        table_exists = True
        table_status = describe_table_response['Table']['TableStatus']
        logging.info('Table "{}" in region "{}" is in "{}" status.'.format(table_name, region_name, table_status))
        if table_status == 'ACTIVE':
            logging.info('Table exists and is ready to use.')
        else:
            logging.info('Table creation ignored.')
    except ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            logging.info('Table "{}" does not exist in region "{}".'.format(table_name, region_name))
        else:
            logging.error(error)
    # Create the table if it does not exist
    if not table_exists:
        logging.info('Creating table "{}" in region "{}"...'.format(table_name, region_name))
        create_table_response = ddb_client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'FileName',
                    'AttributeType': 'S'
                }
            ],
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'FileName',
                    'KeyType': 'HASH'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        table_status = create_table_response['TableDescription']['TableStatus']
        if table_status == 'ACTIVE':
            logging.info('Table created successfully and is ready to use.')
        else:
            counter = 1
            while True:
                logging.info('Sleeping for 5 seconds to re-check table creation status...')
                time.sleep(5)
                describe_table_response = ddb_client.describe_table(
                    TableName=table_name
                )
                table_status = describe_table_response['Table']['TableStatus']
                if table_status == 'ACTIVE':
                    logging.info('Table created successfully and is ready to use.')
                    break
                counter += 1
                if counter == 60:
                    logging.info('Table creation status unknown. Status check exited.')
                    break


def delete_bda_blueprint(bda_client, blueprint_arn, blueprint_version_arn) -> None:
    """
    Function to delete the specified BDA blueprint.

    Parameters:
    bda_client (DynamoDB.Client): The boto3 client for BDA
    blueprint_arn (str): The ARN of the blueprint
    blueprint_arn (str): The ARN of the blueprint version

    Returns:
    None
    """
    # Get the AWS Region name from the boto3 client
    region_name = bda_client.meta.region_name
    if len(blueprint_version_arn) == 0:
        logging.info('Deleting BDA blueprint with ARN "{}" in region "{}"...'
                     .format(blueprint_arn, region_name))
        bda_client.delete_blueprint(blueprintArn=blueprint_arn)
        logging.info('Completed deleting BDA blueprint.')
    else:
        logging.info('Deleting version "{}" of the BDA blueprint with ARN "{}" in region "{}"...'
                     .format(blueprint_version_arn, blueprint_arn, region_name))
        bda_client.delete_blueprint(blueprintArn=blueprint_arn,
                                    blueprintVersion=blueprint_version_arn)
        logging.info('Completed deleting version of the BDA blueprint.')


def delete_bda_project(bda_client, project_arn) -> None:
    """
    Function to delete the specified BDA project.

    Parameters:
    bda_client (DynamoDB.Client): The boto3 client for BDA
    project_arn (str): The ARN of the project

    Returns:
    None
    """
    # Get the AWS Region name from the boto3 client
    region_name = bda_client.meta.region_name
    # Delete the BDA project
    logging.info('Deleting BDA project with ARN "{}" in region "{}"...'.format(project_arn, region_name))
    bda_client.delete_data_automation_project(
        projectArn=project_arn
    )
    counter = 1
    while True:
        logging.info('Sleeping for 5 seconds to check if BDA project has been deleted...')
        time.sleep(5)
        # Check if the table exists
        try:
            bda_client.get_data_automation_project(
                projectArn=project_arn,
                projectStage='LIVE'
            )
        except ClientError as error:
            if error.response['Error']['Code'] == 'ResourceNotFoundException':
                logging.info('BDA project with ARN "{}" does not exist in region "{}".'.format(project_arn,
                                                                                               region_name))
            else:
                logging.error(error)
            break
        counter += 1
        if counter == 60:
            logging.info('BDA project deletion status unknown. Status check exited.')
            break


def delete_metadata_table(ddb_client, table_name) -> None:
    """
    Function to delete the specified metadata table in Amazon DynamoDB that stores tags related to specific files.

    Parameters:
    ddb_client (DynamoDB.Client): The boto3 client for DynamoDB
    table_name (str): The name of the table

    Returns:
    None
    """
    # Get the AWS Region name from the boto3 client
    region_name = ddb_client.meta.region_name
    # Delete the table
    logging.info('Deleting table "{}" in region "{}"...'.format(table_name, region_name))
    ddb_client.delete_table(
        TableName=table_name
    )
    counter = 1
    while True:
        logging.info('Sleeping for 5 seconds to check if table has been deleted...')
        time.sleep(5)
        # Check if the table exists
        try:
            ddb_client.describe_table(
                TableName=table_name
            )
        except ClientError as error:
            if error.response['Error']['Code'] == 'ResourceNotFoundException':
                logging.info('Table "{}" does not exist in region "{}".'.format(table_name, region_name))
            else:
                logging.error(error)
            break
        counter += 1
        if counter == 60:
            logging.info('Table deletion status unknown. Status check exited.')
            break


def write_to_metadata_table(ddb_resource, table_name, file_full_s3_path, file_metadata) -> None:
    """
    Function to write the specified metadata using the specified S3 path as the key to the metadata table.

    Parameters:
    ddb_resource (DynamoDB.Client resource client): The boto3 resource client for DynamoDB
    table_name (str): The name of the table
    file_full_s3_path (str): The full path to the S3 file i.e. the S3 URI
    file_metadata (str): The metadata to write in JSON string format

    Returns:
    None
    """
    ddb_table = ddb_resource.Table(table_name)
    logging.info('Writing to table "{}" with key "{}"...'.format(table_name, file_full_s3_path))
    ddb_table.put_item(
        Item={
            'FileName': file_full_s3_path,
            'Metadata': json.loads(file_metadata)
        }
    )
    logging.info('Completed writing to table.')


def retrieve_from_metadata_table(ddb_resource, table_name, file_full_s3_path) -> str:
    """
    Function to retrieve the metadata using the specified S3 path as the key to the metadata table.

    Parameters:
    ddb_resource (DynamoDB.Client resource client): The boto3 resource client for DynamoDB
    table_name (str): The name of the table
    file_full_s3_path (str): The full path to the S3 file i.e. the S3 URI

    Returns:
    file_metadata (str): The retrieved metadata in JSON string format
    """
    ddb_table = ddb_resource.Table(table_name)
    logging.debug('Retrieving from table "{}" with key "{}"...'.format(table_name, file_full_s3_path))
    response = ddb_table.get_item(
        Key={
            'FileName': file_full_s3_path
        }
    )
    logging.debug('Completed retrieving from table.')
    file_metadata = json.dumps(response['Item']['Metadata'])
    return file_metadata


def get_file_name_and_extension(file_full_path) -> tuple[str, str]:
    """
    Function to get the name and extension from the specified file path.

    Parameters:
    file_full_path (str): The file path

    Returns:
    tuple[str, str]: The file name and extension
    """
    file_name = ''
    file_extension = ''
    file_path_components = file_full_path.split('/')
    file_name_and_extension = file_path_components[-1].rsplit('.', 1)
    if len(file_name_and_extension) > 0:
        file_name = file_name_and_extension[0]
        if len(file_name_and_extension) > 1:
            file_extension = file_name_and_extension[1]
    return file_name, file_extension


def is_supported_file_type(data_file_full_path) -> bool:
    """
    Function to check if the specified data file is a supported type by Converse API or not.

    Parameters:
    data_file_full_path (str): The full path to the data file

    Returns:
    bool: The flag that indicates if this is a supported type or not
    """
    supported_file_types = ['pdf', 'csv', 'doc', 'docx', 'xls', 'xlsx', 'html', 'txt', 'md',
                            'png', 'jpeg', 'gif', 'webp']
    data_file_name, data_file_extension = get_file_name_and_extension(data_file_full_path)
    if data_file_extension in supported_file_types:
        logging.debug("The specified file '{}' of type '{}' is supported by Converse API.".format(data_file_full_path,
                                                                                                  data_file_extension))
        return True
    else:
        logging.warning("The specified file '{}' of type '{}' is not supported by Converse API.".format(data_file_full_path,
                                                                                                        data_file_extension))
        logging.info("Converse API supported file types are {}".format(supported_file_types))
        return False


def read_file(file_full_path, file_read_type) -> str | bytes:
    """
    Function to get the read the content of the specified file.

    Parameters:
    file_full_path (str): The path to the file
    file_read_type (str): File read type - r or rb

    Returns:
    string | bytes: The file content as string or bytes
    """
    with open(file_full_path, file_read_type) as file:
        file_content = file.read()
    return file_content


def invoke_llm(model_or_inference_profile_id, bedrock_rt_client, system_prompts, messages, log_prompt_response) -> str:
    """
    Function to invoke the specified LLM through Amazon Bedrock's Converse API

    Parameters:
    model_or_inference_profile_id (str): The id of the LLM in Bedrock
    bedrock_rt_client (BedrockRuntime.Client): The boto3 client for Bedrock Runtime
    system_prompts list[str]: The list of system prompts to the LLM
    messages list[str]: The list of messages specified as the user prompt to the LLM
    log_prompt_response (bool): The flag to enable/disable logging of the prompt response

    Returns:
    str: The prompt response from the LLM
    """
    # Set the inference parameters
    inference_config = {
        "temperature": 0
    }
    additional_model_fields = None
    # Invoke the LLM
    logging.info('Invoking LLM "{}" with specified inference parameters "{}" and additional model fields "{}"...'.
                 format(model_or_inference_profile_id, inference_config, additional_model_fields))
    response = bedrock_rt_client.converse(
        modelId=model_or_inference_profile_id,
        messages=messages,
        system=system_prompts,
        inferenceConfig=inference_config,
        additionalModelRequestFields=additional_model_fields
    )
    logging.info('Completed invoking LLM.')
    token_usage = response['usage']
    logging.info("Input tokens: {}".format(token_usage['inputTokens']))
    logging.info("Output tokens: {}".format(token_usage['outputTokens']))
    logging.info("Total tokens: {}".format(token_usage['totalTokens']))
    logging.info("Stop reason: {}".format(response['stopReason']))
    metrics = response['metrics']
    logging.info('Prompt latency = {} second(s)'.format(int(metrics['latencyMs']) / 1000))
    prompt_response = response['output']['message']['content'][0]['text']
    # Log the response
    if log_prompt_response:
        logging.info('PROMPT: {}'.format(messages[0]['content'][0]))
        logging.info('RESPONSE: {}'.format(prompt_response))
    # Return the LLM response text
    return prompt_response


def process_prompt(model_or_inference_profile_id, bedrock_rt_client, prompt_templates_dir,
                   system_prompt_template_file, user_prompt_template_file,
                   data_file_full_path) -> str:
    """
    Function that processes the prompt by invoking the specified LLM

    Parameters:
    model_or_inference_profile_id (str): The id of the model or the inference profile in Bedrock
    bedrock_rt_client (boto3 client): The boto3 client for Bedrock Runtime
    prompt_templates_dir (str): The directory that contains the prompt templates
    system_prompt_template_file (str): The name of the system prompt template file
    user_prompt_template_file (str): The name of the user prompt template file
    data_file_full_path (str): The full path for the data file

    Returns:
    str: The prompt response from the LLM
    """
    # Read the prompt template and perform variable substitution
    system_prompts = [
        {
            "text": read_file(os.path.join(prompt_templates_dir, system_prompt_template_file), 'r')
        }
    ]
    data_file_name, data_file_extension = get_file_name_and_extension(data_file_full_path)
    data_file_extension = data_file_extension.lower()
    if data_file_extension in ['png', 'jpeg', 'gif', 'webp']:
        content_details = {
            "image": {
                "format": data_file_extension,
                "source": {
                    "bytes": read_file(data_file_full_path, 'rb')
                }
            }
        }
    else:
        content_details = {
            "document": {
                "name": data_file_name,
                "format": data_file_extension,
                "source": {
                    "bytes": read_file(data_file_full_path, 'rb')
                }
            }
        }
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "text": read_file(os.path.join(prompt_templates_dir, user_prompt_template_file), 'r')
                },
                content_details
            ]
        }
    ]
    # Invoke the LLM and return the response
    llm_response = invoke_llm(model_or_inference_profile_id, bedrock_rt_client, system_prompts, messages, True)
    llm_response = llm_response.lstrip("```json")
    llm_response = llm_response.rstrip("```")
    llm_response = llm_response.lstrip("<output_format>")
    llm_response = llm_response.rstrip("</output_format>")
    llm_response_json = json.loads(llm_response)
    llm_response_json['comments'] = ('Created using the LLM with id "{}" on Amazon Bedrock.'
                                     .format(model_or_inference_profile_id))
    llm_response_json['create_date_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return json.dumps(llm_response_json)


def process_bda_invocation(bda_rt_client, input_s3_file, s3_output_bucket_name, bda_project_arn) -> str:
    """
    Function that processes the input file using BDA

    Parameters:
    bda_rt_client (boto3 client): The boto3 client for Bedrock Data Automation Runtime
    input_s3_file (str): The S3 file path along with its prefix
    s3_output_bucket_name (str): The name of the output S3 bucket
    bda_project_arn (str): The ARN of the BDA project

    Returns:
    str: The BDA invocation id
    """
    # Async invoke BDA
    bda_invocation_response = bda_rt_client.invoke_data_automation_async(
        inputConfiguration={
            's3Uri': 's3://{}'.format(input_s3_file)
        },
        outputConfiguration={
            's3Uri': 's3://{}'.format(s3_output_bucket_name)
        },
        dataAutomationConfiguration={
            'dataAutomationArn': bda_project_arn,
            'stage': 'LIVE'
        }
    )
    # Get the invocation ARN
    invocation_arn = bda_invocation_response['invocationArn']
    # Get the invocation id
    invocation_id = substring_after(invocation_arn, '/')
    # Sleep and check the invocation status; exit loop accordingly
    counter = 1
    while True:
        logging.info('Sleeping for 5 seconds to re-check BDA invocation status...')
        time.sleep(5)
        response1 = bda_rt_client.get_data_automation_status(
            invocationArn=invocation_arn
        )
        status = response1['status']
        logging.info('BDA invocation status is "{}"'.format(status))
        if status in ['Success']:
            logging.info('BDA invocation id is "{}"'.format(invocation_id))
            return invocation_id
        elif status in ['ServiceError', 'ClientError']:
            logging.error('{} occurred during BDA invocation.'.format(status))
            return ''
        counter += 1
        if counter == 60:
            logging.warning('BDA invocation status unknown. Status check exited.')
            return ''


def process_bda_result(invocation_id, s3_output_bucket_name, local_dir_name_prefix) -> str:
    """
    Function that processes the input file using BDA

    Parameters:
    bda_rt_client (boto3 client): The boto3 client for Bedrock Data Automation Runtime
    input_s3_file (str): The S3 file path along with its prefix
    s3_output_bucket_name (str): The name of the output S3 bucket
    bda_project_arn (str): The ARN of the BDA project

    Returns:
    str: The metadata as a JSON string
    """
    logging.info('Processing BDA invocation result for invocation id "{}"...'.format(invocation_id))
    # Create the local temp directory
    local_dir = os.path.join(local_dir_name_prefix, invocation_id)
    os.makedirs(local_dir, exist_ok=True)
    # Download all the BDA result files from S3 using the invocation id
    download_from_s3(local_dir, s3_output_bucket_name, '/{}/'.format(invocation_id))
    # Read the downloaded 'job_metadata.json' file
    with open(os.path.join(local_dir, 'job_metadata.json'), 'r') as file:
        bda_result_metadata = json.load(file)
    # Get the standard output and the optional custom output (through the Blueprint)
    bda_result_metadata_segment = bda_result_metadata['output_metadata'][0]['segment_metadata'][0]
    bda_result_standard_output_s3_file = bda_result_metadata_segment['standard_output_path']
    # Read the downloaded standard output file
    bda_result_standard_output_local_file = local_dir + substring_after(bda_result_standard_output_s3_file,
                                                                        invocation_id)
    with open(os.path.join(local_dir, bda_result_standard_output_local_file), 'r') as file:
        bda_result_standard_output = json.load(file)
    # Get the flag that indicates if a custom output was generated
    bda_result_custom_output_status = bda_result_metadata_segment['custom_output_status']
    # A 'MATCH' indicates that a Blueprint was applied and a custom output was generated
    if bda_result_custom_output_status == 'MATCH':
        bda_result_custom_output_s3_file = bda_result_metadata_segment['custom_output_path']
        # Read the optionally downloaded custom output file
        bda_result_custom_output_local_file = local_dir + substring_after(bda_result_custom_output_s3_file,
                                                                          invocation_id)
        with open(os.path.join(local_dir, bda_result_custom_output_local_file), 'r') as file:
            bda_result_custom_output = json.load(file)
        # Get the data from the custom output
        pii_indicator = bda_result_custom_output['inference_result']['pii_indicator']
        pii_explanation = bda_result_custom_output['inference_result']['pii_explanation']
    else:
        pii_indicator = "None"
        pii_explanation = ""
    # Create the file metadata based on if it is DOCUMENT or IMAGE
    if 'document' in bda_result_standard_output:
        file_metadata = {
            "description": bda_result_standard_output['document']['description'],
            "summary": bda_result_standard_output['document']['summary'],
            "pii_indicator": pii_indicator,
            "pii_explanation": pii_explanation,
            "comments": "Created using Amazon Bedrock Data Automation.",
            "create_date_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    else:
        file_metadata = {
            "description": "Image",
            "summary": bda_result_standard_output['image']['summary'],
            "pii_indicator": pii_indicator,
            "pii_explanation": pii_explanation,
            "comments": "Created using Amazon Bedrock Data Automation.",
            "create_date_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    logging.info('Completed processing BDA invocation result.')
    return json.dumps(file_metadata)
