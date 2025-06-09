from typing import Any

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue

from .report import average_open_days, count_issue_types


def create_markdown_report(context: AssetExecutionContext, report_data: dict[str, dict]) -> str:
    """Create a markdown report from the report data.

    Args:
        - context (AssetExecutionContext): \
            Asset execution context.
        - report_data (dict[str, dict]): \
            Data for the report as a dictionary.

    Returns:
        - str: \
            Markdown formatted report.
    """
    # use pandas to convert dict with report data to markdown table
    df_report = pd.DataFrame.from_dict(report_data)
    md_report = df_report.to_markdown()

    context.add_output_metadata(
        metadata={
            'report': MetadataValue.md(md_report),
        }
    )

    return md_report


def extract_metadata(repo_metadata: dict[str, Any]) -> dict[str, Any]:
    """Extracts relevant fields from merged repository data and adds them to the report data dict.

    Args:
        merged_data (dict[str, Any]):
            Merged data for a GitHub repository, with keys:
                - 'metadata': Repository metadata as returned by GitHub API.
                - 'releases': List of releases as returned by GitHub API.
                - 'issues': List of issues/PRs as returned by GitHub API.

    Returns:
        dict[str, Any]: Extracted data from one repository as a column for the report.
    """
    repo_data = repo_metadata['metadata']
    all_releases = repo_metadata.get('releases', [])
    all_issues = repo_metadata.get('issues', [])

    # Count open/closed issues and PRs
    issue_counts = count_issue_types(all_issues, filter_type="issues")
    pr_counts = count_issue_types(all_issues, filter_type="pull_requests")

    # Calculate average days open for issues and PRs
    avg_days_issue = average_open_days(all_issues, filter_type="issues")
    avg_days_pr = average_open_days(all_issues, filter_type="pull_requests")

    # set all fields for the report
    extracted_data = {
        'stars': repo_data.get('stargazers_count'),
        'forks': repo_data.get('forks_count'),
        'watchers': repo_data.get('subscribers_count'),
        'releases': len(all_releases),
        'open issues': issue_counts.get('open', 0),
        'closed issues': issue_counts.get('closed', 0),
        'avg days until issue was closed': avg_days_issue,
        'open PRs': pr_counts.get('open', 0),
        'closed PRs': pr_counts.get('closed', 0),
        'avg days until PR was closed': avg_days_pr,
    }

    return extracted_data

