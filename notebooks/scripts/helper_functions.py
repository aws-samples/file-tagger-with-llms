"""
Copyright 2025 Amazon.com, Inc. or its affiliates.  All Rights Reserved.
SPDX-License-Identifier: MIT-0
"""
import boto3
from botocore.exceptions import ClientError
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
            logging.warning('Table exists and is ready to use.')
        else:
            logging.warning('Table creation ignored.')
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
                logging.info('Sleeping for 5 seconds to re-check table status...')
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
    return llm_response

