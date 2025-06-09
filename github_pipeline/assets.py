from typing import Any

from dagster import (AssetExecutionContext, AssetIn, AssetKey, FreshnessPolicy,
                     MetadataValue, asset)

from .resources import GitHubAPIResource
from .utils import create_markdown_report, extract_metadata


@asset(
    name='delta-rs_repo_metadata',
    key_prefix=['stage', 'github', 'repositories', 'delta-io', 'delta-rs'],
    io_manager_key='json_io_manager',
    group_name='github',
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24),  # 24 hours
)
def delta_rs_metadata(context: AssetExecutionContext, github_api: GitHubAPIResource) -> dict[str, Any]:
    """Metadata from the GitHub repository of the Delta Lake Python client."""
    owner = 'delta-io'
    repo = 'delta-rs'

    # Fetch all data
    repo_metadata = github_api.get_repository(owner=owner, repo=repo)
    all_releases = github_api.get_all_releases(owner=owner, repo=repo)
    all_issues = github_api.get_all_issues(owner=owner, repo=repo)

    # Combine into one dictionary
    merged_data = {
        'metadata': repo_metadata,
        'releases': all_releases,
        'issues': all_issues,
    }

    # Optional: add metadata to context
    context.add_output_metadata(
        metadata={
            'repo link': MetadataValue.url(repo_metadata.get('html_url')),
            'data preview': MetadataValue.json({
                'metadata': {
                    'name': repo_metadata.get('name'),
                    'description': repo_metadata.get('description'),
                },
                'releases_count': len(all_releases),
                'issues_count': len(all_issues),
            }),
        }
    )

    return merged_data


@asset(
    name='iceberg-python_repo_metadata',
    key_prefix=['stage', 'github', 'repositories', 'apache', 'iceberg-python'],
    io_manager_key='json_io_manager',
    group_name='github',
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24),  # 24 hours
)
def iceberg_python_metadata(
    context: AssetExecutionContext,
    github_api: GitHubAPIResource,
) -> dict[str, Any]:
    """Metadata, releases, and issues from the GitHub repository of the Iceberg Python client."""
    owner = 'apache'
    repo = 'iceberg-python'

    # Fetch all data
    repo_metadata = github_api.get_repository(owner=owner, repo=repo)
    all_releases = github_api.get_all_releases(owner=owner, repo=repo)
    all_issues = github_api.get_all_issues(owner=owner, repo=repo)

    # Combine into one dictionary
    merged_data = {
        'metadata': repo_metadata,
        'releases': all_releases,
        'issues': all_issues,
    }

    # Optional: add metadata to context
    context.add_output_metadata(
        metadata={
            'repo link': MetadataValue.url(repo_metadata.get('html_url')),
            'data preview': MetadataValue.json({
                'metadata': {
                    'name': repo_metadata.get('name'),
                    'description': repo_metadata.get('description'),
                },
                'releases_count': len(all_releases),
                'issues_count': len(all_issues),
            }),
        }
    )

    return merged_data


@asset(
    name='hudi-rs_repo_metadata',
    key_prefix=['stage', 'github', 'repositories', 'apache', 'hudi-rs'],
    io_manager_key='json_io_manager',
    group_name='github',
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60 * 24),  # 24 hours
)
def hudi_rs_metadata(context: AssetExecutionContext, github_api: GitHubAPIResource) -> dict[str, Any]:
    """Metadata from the GitHub repository of the Hudi Python client."""
    # TODO: enhance error propagation of get_repository method
    owner = 'apache'
    repo = 'hudi-rs'

    # Fetch all data
    repo_metadata = github_api.get_repository(owner=owner, repo=repo)
    all_releases = github_api.get_all_releases(owner=owner, repo=repo)
    all_issues = github_api.get_all_issues(owner=owner, repo=repo)

    # Combine into one dictionary
    merged_data = {
        'metadata': repo_metadata,
        'releases': all_releases,
        'issues': all_issues,
    }

    # Optional: add metadata to context
    context.add_output_metadata(
        metadata={
            'repo link': MetadataValue.url(repo_metadata.get('html_url')),
            'data preview': MetadataValue.json({
                'metadata': {
                    'name': repo_metadata.get('name'),
                    'description': repo_metadata.get('description'),
                },
                'releases_count': len(all_releases),
                'issues_count': len(all_issues),
            }),
        }
    )

    return merged_data


@asset(
    key_prefix=['dm', 'reports'],
    ins={
        'delta_rs': AssetIn(AssetKey('delta-rs_repo_metadata')),
        'iceberg_python': AssetIn(AssetKey('iceberg-python_repo_metadata')),
        'hudi_rs': AssetIn(AssetKey('hudi-rs_repo_metadata')),
    },
    io_manager_key='md_io_manager',
    group_name='github',
)
def repo_report(
    context: AssetExecutionContext, delta_rs: dict[str, Any], iceberg_python: dict[str, Any], hudi_rs: dict[str, Any]
) -> str:
    """Report for comparing GitHub repositories."""
    report_data = {
        'delta-rs': extract_metadata(repo_metadata=delta_rs),
        'iceberg-python': extract_metadata(repo_metadata=iceberg_python),
        'hudi-rs': extract_metadata(repo_metadata=hudi_rs),
    }

    return create_markdown_report(context=context, report_data=report_data)
