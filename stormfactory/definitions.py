from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules

from . import assets

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    asset_checks=load_asset_checks_from_modules([assets]),
)
