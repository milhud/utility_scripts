# -*- coding: utf-8 -*-
"""CONUS404 Iterative Downloader - Downloads data month by month"""

import planetary_computer
import xarray as xr
import pystac_client
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import calendar

# -----------------------
# Configuration
# -----------------------
YEARS = [1988]  # list of years
VARIABLES = ["T2", "Q2"]  # variables to download
OUTPUT_DIR = "conus404_data"

# -----------------------
# Connect to Planetary Computer STAC
# -----------------------
try:
    # newer pystac-client supports modifier=
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign,
    )
except TypeError:
    # fallback for older pystac-client (no modifier param)
    catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
    # sign manually
    catalog = planetary_computer.sign(catalog)

# get the CONUS404 collection
collection = catalog.get_collection("conus404")

# find the correct asset key (zarr or zarr-abfs)
asset_key = "zarr-abfs" if "zarr-abfs" in collection.assets else "zarr"
asset = collection.assets[asset_key]

# open the zarr dataset
ds = xr.open_zarr(
    asset.href,
    storage_options=asset.extra_fields.get("xarray:storage_options", {}),
    **asset.extra_fields.get("xarray:open_kwargs", {}),
)


# -----------------------
# Download helper function
# -----------------------
def download_variable_month(year, month, variable):
    print(f"(downloading {variable} for {year}-{month:02d})")

    start_date = f"{year}-{month:02d}-01"
    last_day = calendar.monthrange(year, month)[1]
    end_date = f"{year}-{month:02d}-{last_day}"

    # time slice
    ds_slice = ds.sel(time=slice(start_date, end_date))

    if variable not in ds_slice.data_vars:
        raise KeyError(f"Variable '{variable}' not found in dataset. Available: {list(ds_slice.data_vars)}")

    ds_variable = ds_slice[[variable]]

    # (optional) remove .load() to avoid memory issues
    # ds_loaded = ds_variable.load()

    # output path
    month_dir = os.path.join(OUTPUT_DIR, str(year), f"{month:02d}")
    os.makedirs(month_dir, exist_ok=True)

    # save to NetCDF
    output_file = os.path.join(month_dir, f"{variable}.nc")
    ds_variable.to_netcdf(output_file)

    print(f"({variable} for {year}-{month:02d} complete)")
    return output_file


# -----------------------
# Parallel download loop
# -----------------------
tasks = [(year, month, variable) for year in YEARS for month in range(1, 13) for variable in VARIABLES]

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(download_variable_month, y, m, v): (y, m, v) for y, m, v in tasks}

    for future in as_completed(futures):
        year, month, variable = futures[future]
        try:
            future.result()
        except Exception as exc:
            print(f"ERROR: {variable} for {year}-{month:02d} failed: {exc}")
