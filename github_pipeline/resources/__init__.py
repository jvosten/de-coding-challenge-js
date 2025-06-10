from dagster import EnvVar
from dagster_aws.s3 import S3PickleIOManager, S3Resource, s3_resource

from .gh_resource import GitHubAPIResource
from .s3_json_io_manager import S3JsonIOManager
from .s3_md_io_manager import S3MdIOManager

# Resource that gives access to S3.
# S3Resource provides a get_client method that returns a boto3 client for interacting with S3.
# For our purposes, we will use the S3Resource to pass just the credentials to our IOManagers.
# This works because S3Resource is a subclass of ResourceWithS3Configuration, which is what
# we use in our IOManagers to configure s3fs

s3_resource = S3Resource(
    aws_access_key_id=EnvVar('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=EnvVar('AWS_SECRET_ACCESS_KEY'),
    endpoint_url=EnvVar('AWS_ENDPOINT_URL_S3')
)


RESOURCES_DEV = {
    'github_api': GitHubAPIResource(
        github_token=EnvVar('GITHUB_TOKEN').get_value()
    ),
    'io_manager': S3PickleIOManager(
        s3_resource=s3_resource,
        s3_bucket=EnvVar('S3_BUCKET_NAME')
    ),
    'json_io_manager': S3JsonIOManager(
        s3_bucket=EnvVar('S3_BUCKET_NAME'),
        s3_prefix='',
        s3_configuration=s3_resource
    ),
    'md_io_manager': S3MdIOManager(
        s3_bucket=EnvVar('S3_BUCKET_NAME'),
        s3_prefix='',
        s3_configuration=s3_resource
    ),
}
