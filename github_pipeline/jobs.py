from dagster import AssetSelection, define_asset_job, load_assets_from_modules

from .assets import github_data, report

all_assets = load_assets_from_modules([github_data, report])

# Job for retrieving GitHub statistics
github_job = define_asset_job(
    name='refresh_repository_report',
    selection=AssetSelection.all()
)
