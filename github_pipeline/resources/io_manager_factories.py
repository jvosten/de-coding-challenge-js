from abc import ABC, abstractmethod

from dagster import ConfigurableIOManagerFactory, ResourceDependency
from dagster_aws.s3.resources import ResourceWithS3Configuration
from pydantic import Field
from upath import UPath


# Inspired from: https://github.com/codecentric/dagster-blog-post-working-with-io-
#                    manager/blob/main/core/resources/io_manager_factories.py
class S3UPathIOManagerFactory(ConfigurableIOManagerFactory, ABC):
    """An abstract base class for creating IO managers that interact with S3 storage.

    This class provides a framework for creating IO managers that can be configured
    to work with S3 storage. It inherits from `ConfigurableIOManagerFactory` and is
    designed to be extended by subclasses that implement specific IO manager types.

    Attributes:
        s3_bucket (str): The S3 bucket to use for the IO manager. This attribute
            specifies the name of the S3 bucket where the IO manager will read
            from or write to.
        s3_prefix (str): The S3 prefix to use for the IO manager. This attribute
            specifies the prefix within the S3 bucket that the IO manager will
            use to organize its data.
        s3_configuration (ResourceDependency[ResourceWithS3Configuration]): A
            dependency on a resource that provides S3 configuration details,
            including access keys and endpoint URLs. This attribute is used
            to configure the IO manager's connection to S3.
    """

    s3_bucket: str = Field(description='The S3 bucket to use for the IO manager.')
    s3_prefix: str = Field(description='The S3 prefix to use for the IO manager.')
    s3_configuration: ResourceDependency[ResourceWithS3Configuration]

    @abstractmethod
    def io_manager_class(self):
        """Return the class of the IO manager that this factory will create.

        This abstract method must be implemented by subclasses to return the class
        of the IO manager that this factory will create. The returned class should
        be a subclass of `UPathIOManager` or a similar base class for IO managers
        that interact with S3 storage.

        Returns:
            type: The class of the IO manager that this factory will create.
        """
        raise NotImplementedError

    def create_io_manager(self, context):
        """Create and return an instance of the IO manager class.

        The IO manager is configured with the S3 bucket, prefix, and configuration
        details provided by this factory.

        Args:
            context: The context under which the IO manager is being created.

        Returns:
            The instance of the IO manager class specified by `io_manager_class`.
        """
        return self.io_manager_class()(
            base_path=UPath(
                # Construct the S3 path from the bucket and prefix
                f's3://{self.s3_bucket}/{self.s3_prefix}',
                # Configure the IO manager with the S3 configuration
                key=self.s3_configuration.aws_access_key_id,
                secret=self.s3_configuration.aws_secret_access_key,
                client_kwargs={
                    'endpoint_url': self.s3_configuration.endpoint_url,
                },
            )
        )
