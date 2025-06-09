from collections import Counter
from datetime import datetime
from statistics import mean
from typing import Any, Literal, Optional

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue


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


def filter_items(
    items: list[dict[str, Any]],
    filter_type: Literal["issues", "pull_requests"],
) -> list[dict[str, Any]]:
    """Filters a list of GitHub API response dicts by issue type.

    Args:
        items: list of GitHub issue/pull request dicts.
        filter_type:
            - "issues": Only return items without a "pull_request" key (real issues).
            - "pull_requests": Only return items with a "pull_request" key (pull requests).

    Returns:
        list[dict[str, Any]]: Filtered list of items.

    Raises:
        ValueError: If filter_type is not "issues" or "pull_requests".
    """
    if filter_type not in ("issues", "pull_requests"):
        raise ValueError('filter_type must be "issues" or "pull_requests"')
    if filter_type == "issues":
        return [d for d in items if "pull_request" not in d]
    else:  # "pull_requests"
        return [d for d in items if "pull_request" in d]

def count_issue_types(
    items: list[dict[str, Any]],
    filter_type: Literal["issues", "pull_requests"] = "issues",
) -> dict[str, int]:
    """Counts the number of open and closed items (issues or pull requests) in a list of GitHub API response dicts.

    Args:
        items: list of GitHub issue/pull request dicts.
        filter_type:
            - "issues": Only count items without a "pull_request" key (real issues).
            - "pull_requests": Only count items with a "pull_request" key (pull requests).

    Returns:
        dict[str, int]: Counts of open and closed items (keys: "open", "closed").
    """
    filtered = filter_items(items, filter_type)
    state_counts = Counter(d["state"] for d in filtered)
    return {
        "open": state_counts.get("open", 0),
        "closed": state_counts.get("closed", 0),
    }

def average_open_days(
    items: list[dict[str, Any]],
    filter_type: Literal["issues", "pull_requests"] = "issues",
) -> Optional[float]:
    """Calculates the average number of days issues or PRs were open.

    Args:
        items: list of GitHub API response dicts.
        filter_type: 'issues' or 'pull_requests'. Determines which items to include.

    Returns:
        Average days open (float, rounded to one decimal), or None if no valid items.
    """
    filtered = filter_items(items, filter_type)
    valid_items = []
    for item in filtered:
        if not all(k in item and item[k] is not None for k in ("created_at", "closed_at")):
            continue
        try:
            created = datetime.fromisoformat(item["created_at"].rstrip("Z"))
            closed = datetime.fromisoformat(item["closed_at"].rstrip("Z"))
            days_open = (closed - created).days
            valid_items.append(days_open)
        except (ValueError, TypeError):
            continue
    if not valid_items:
        return None
    return round(mean(valid_items), 1)

