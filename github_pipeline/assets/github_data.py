from collections.abc import Sequence
from typing import Any, Dict, List, Tuple

import yaml
from dagster import (
    AssetExecutionContext,
    AssetOut,
    AssetsDefinition,
    FreshnessPolicy,
    MetadataValue,
    file_relative_path,
    multi_asset,
)

from ..resources import GitHubAPIResource


def build_multi_repo_data(
    owner: str,
    repo: str,
    io_manager_key: str = "json_io_manager",
    freshness_policy: FreshnessPolicy = FreshnessPolicy(maximum_lag_minutes=60 * 24),
    group_name: str = "github",
) -> AssetsDefinition:
    """Factory for creating a multi-asset that fetches repository data.

    Args:
        owner: Repository owner (e.g., 'delta-io').
        repo: Repository name (e.g., 'delta-rs').
        io_manager_key: IO manager key for storing outputs.
        freshness_policy: Freshness policy for outputs.
        group_name: Group name for assets.

    Returns:
        A multi-asset function for the given owner/repo.
    """
    sanitized_repo = repo.replace('-', '_') # In asset keys '-' is not permitted
    asset_name = f"{sanitized_repo}_repo_data"
    outs = {
        f"{sanitized_repo}_metadata": AssetOut(
            key_prefix=["stage", "github", "repositories", owner, repo],
            io_manager_key=io_manager_key,
            metadata={"description": f"Metadata from the GitHub repository {owner}/{repo}."},
            freshness_policy=freshness_policy,
        ),
        f"{sanitized_repo}_releases": AssetOut(
            key_prefix=["stage", "github", "repositories", owner, repo],
            io_manager_key=io_manager_key,
            metadata={"description": f"All releases from the GitHub repository {owner}/{repo}."},
            freshness_policy=freshness_policy,
        ),
        f"{sanitized_repo}_issues": AssetOut(
            key_prefix=["stage", "github", "repositories", owner, repo],
            io_manager_key=io_manager_key,
            metadata={"description": f"All issues and pull requests from the GitHub repository {owner}/{repo}."},
            freshness_policy=freshness_policy,
        ),
    }

    @multi_asset(
        name=asset_name,
        outs=outs,
        group_name=group_name,
    )
    def multi_repo_data(
        context: AssetExecutionContext,
        github_api: GitHubAPIResource,
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Fetches and yields metadata, releases, and issues from the GitHub repository."""
        repo_metadata = github_api.get_repository(owner=owner, repo=repo)
        all_releases = github_api.get_all_releases(owner=owner, repo=repo)
        all_issues = github_api.get_all_issues(owner=owner, repo=repo)

        context.add_output_metadata(
            output_name=f"{sanitized_repo}_metadata",
            metadata={
                "repo link": MetadataValue.url(repo_metadata.get("html_url", "")),
                "description": MetadataValue.text(repo_metadata.get("description", "")),
            },
        )
        context.add_output_metadata(
            output_name=f"{sanitized_repo}_releases",
            metadata={
                "count": MetadataValue.int(len(all_releases)),
            },
        )
        context.add_output_metadata(
            output_name=f"{sanitized_repo}_issues",
            metadata={
                "count": MetadataValue.int(len(all_issues)),
            },
        )

        return repo_metadata, all_releases, all_issues

    return multi_repo_data


def load_repo_from_yaml(yaml_path: str) -> Sequence[AssetsDefinition]:
    """Loads and builds Dagster multi-assets for each repository defined in a YAML configuration.

    Args:
        yaml_path (str):
            Path to the YAML file containing repository configurations.
            The YAML file should have a 'repositories' key with a list of dictionaries,
            each containing 'owner' and 'repo' keys.

    Returns:
        Sequence[AssetsDefinition]:
            A sequence of Dagster AssetsDefinition objects, each representing a multi-asset
            for a repository defined in the YAML configuration.
    """
    config = yaml.safe_load(open(yaml_path))
    repo_assets = [
        build_multi_repo_data(
            owner=repo_config["owner"],
            repo=repo_config["repo"],
        )
        for repo_config in config["repositories"]
    ]
    return repo_assets

repo_assets = load_repo_from_yaml(
    file_relative_path(__file__, "../resources/repositories.yaml")
)
