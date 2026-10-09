from dagster import Definitions, load_assets_from_modules
from dagster_engine.assets import ingestion, transformations

all_assets = load_assets_from_modules([ingestion, transformations])

defs = Definitions(
    assets=all_assets,
)
