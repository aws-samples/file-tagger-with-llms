## Use AI to build a content-based file tagging system using Amazon Bedrock

This repository contains code samples that will show you how to analyze document and image files stored in S3 using the following techniques:
1. Using Large Language Models (LLMs) hosted on [Amazon Bedrock](https://aws.amazon.com/bedrock/).
2. Using [Amazon Bedrock Data Automation](https://aws.amazon.com/bedrock/bda/).

Both these will show you how to generate metadata based on the content of the files and store them as key-value pairs (tags) in an [Amazon DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html) table with a reference to the files in S3.

### Overview

[Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html) is a popular object storage service on AWS. You can store any type of file as an object in a S3 bucket. Although you can write files of a specific type or context within a specific directory structure (path) in S3, it will be useful to add metadata to the files like it's content description, owner, context etc. so you can easily retrieve the file that you are looking for. There are two ways to do this:

**Option 1: Use the user-defined metadata feature in S3**

While uploading an object to a S3 bucket, you can optionally assign user-defined metadata as key-values pairs to the object. This will be stored along with the object. This cannot be added later on to an existing object. The only way to modify object metadata is to make a copy of the object and set the metadata.

**Option 2: Store the metadata in an external system with a reference to the object in S3**

If you want to set metadata to an existing object in S3 without copying that object or if you want to add to an existing metadata system that already exist, then it will make sense to store the metadata in an external system, like an Amazon DynamoDB table for example. This option is also applicable if the data is stored outside S3 and needs to be tagged with metadata.

In both of these options, if you do not know the metadata that describes the data stored in the object, then, you have to read the object, analyze it's content and generate the appropriate metadata. This is where AI can help.

### To get started

1. Choose an AWS Account to use and make sure to create all resources in that Account.
2. Identify an AWS Region that has [Amazon Bedrock with Amazon Nova 1.0 or Anthropic Claude 3/3.5 or Meta Llama 3.2](https://docs.aws.amazon.com/bedrock/latest/userguide/models-regions.html) models and [Amazon Bedrock Data Automation](https://docs.aws.amazon.com/bedrock/latest/userguide/bda.html).
3. In that Region, create two new or use two existing [Amazon S3 buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html) of your choice. These will be input and output S3 buckets.
4. In the S3 bucket that you designate as the input bucket, upload all the files from the [assets](https://github.com/aws-samples/file-tagger-with-llms/blob/main/assets/) folder.
5. In that same Region, create a new [Amazon SageMaker notebook instance](https://docs.aws.amazon.com/sagemaker/latest/dg/nbi.html) with *Amazon Linux 2, Jupyter Lab 4(notebook-al2-v3)* as the Platform Identifier.
6. Clone this GitHub repo to that notebook instance.
7. In that notebook instance, open the following Jupyter notebooks,
   1. For using Large Language Models (LLMs) hosted on Amazon Bedrock, open *file-tagger-with-bedrock-llms.ipynb* by navigating to the [Amazon SageMaker notebook instances console](https://docs.aws.amazon.com/sagemaker/latest/dg/howitworks-access-ws.html) and clicking on the *Open Jupyter* link.
   2. For using Amazon Bedrock Data Automation, open *file-tagger-with-bedrock-data-automation.ipynb* by navigating to the [Amazon SageMaker notebook instances console](https://docs.aws.amazon.com/sagemaker/latest/dg/howitworks-access-ws.html) and clicking on the *Open Jupyter* link.

### Repository structure

This repository contains

* [Two Jupyter Notebooks](https://github.com/aws-samples/file-tagger-with-llms/blob/main/notebooks/) to get started.

* [A set of helper functions for the notebook](https://github.com/aws-samples/file-tagger-with-llms/blob/main/notebooks/scripts/helper_functions.py)

* [Assets](https://github.com/aws-samples/file-tagger-with-llms/blob/main/assets) folder with files that represent various types of documents and images that will be processed by the notebook. **Note:** Of these, [Document_2.pdf](https://github.com/aws-samples/file-tagger-with-llms/blob/main/assets/Document_2.pdf) is not shared under MIT-0 license.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

