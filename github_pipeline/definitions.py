import os

from dagster import Definitions

from .jobs import all_assets, github_job
from .resources import RESOURCES_DEV

resources_by_deployment_name = {
    'DEV': RESOURCES_DEV
}

deployment_name = os.getenv('DEPLOYMENT_NAME', 'DEV')

defs = Definitions(
    assets=all_assets,
    jobs=[
        github_job
    ],
    resources=resources_by_deployment_name[deployment_name],
)
