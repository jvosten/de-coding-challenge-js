from typing import Any

import yaml
from dagster import AssetExecutionContext, AssetIn, AssetKey, AssetsDefinition, asset, file_relative_path

from ..utils.report import create_markdown_report, extract_metadata


def build_report(
    repo_configs: list[dict[str, Any]],
    io_manager_key: str = "md_io_manager",
    group_name: str = "github",
) -> AssetsDefinition:
    """Factory for creating a Dagster asset that generates a consolidated report from multiple repository assets.

    Args:
        repo_configs (list[dict[str, Any]]): List of repository configurations. Each dict should have a "repo" key.
        io_manager_key (str): IO manager key for storing the report output (default: "md_io_manager").
        group_name (str): Group name for the asset (default: "github").

    Returns:
        AssetsDefinition: A Dagster asset definition that generates a consolidated report from the metadata,
            releases, and issues of all specified repositories.
    """
    ins = {}
    for repo in repo_configs:
        sanitized_repo = repo["repo"].replace("-", "_")
        ins[f"{sanitized_repo}_metadata"] = AssetIn(key=AssetKey([f"{sanitized_repo}_metadata"]))
        ins[f"{sanitized_repo}_releases"] = AssetIn(key=AssetKey([f"{sanitized_repo}_releases"]))
        ins[f"{sanitized_repo}_issues"] = AssetIn(key=AssetKey([f"{sanitized_repo}_issues"]))

    @asset(
        key_prefix=["dm", "reports"],
        ins=ins,
        io_manager_key=io_manager_key,
        group_name=group_name,
    )
    def repo_report(context: AssetExecutionContext, **kwargs: dict[str, Any]) -> str:
        report_data = {}
        for repo in repo_configs:
            sanitized_repo = repo["repo"].replace("-", "_")
            original_repo_name = repo["repo"]
            merged_data = {
                "metadata": kwargs[f"{sanitized_repo}_metadata"],
                "releases": kwargs[f"{sanitized_repo}_releases"],
                "issues": kwargs[f"{sanitized_repo}_issues"],
            }
            report_data[original_repo_name] = extract_metadata(merged_data)

        return create_markdown_report(context=context, report_data=report_data)

    return repo_report


def load_report_from_yaml(yaml_path: str) -> AssetsDefinition:
    """Loads the report asset from a YAML config.

    Args:
        yaml_path: Path to the YAML file.

    Returns:
        AssetsDefinition: The report asset.
    """
    with open(yaml_path, "r") as f:
        repo_configs = yaml.safe_load(f)["repositories"]

    # Create and return the report asset
    return build_report(repo_configs)

report_asset = load_report_from_yaml(
    file_relative_path(__file__, "../resources/repositories.yaml")
)
