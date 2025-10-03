# -*- coding: utf-8 -*-
"""CONUS404 Iterative Downloader - Downloads data month by month"""

import planetary_computer
import xarray as xr
import pystac_client
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import calendar

# configuration
YEARS = [1988]  # comma separted
VARIABLES = ["T2", "Q2"]  # from planetary computer website
OUTPUT_DIR = "conus404_data"

# connect
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace,
)

c = catalog.get_collection("conus404")
asset = c.assets["zarr-abfs"]
ds = xr.open_zarr(
    asset.href,
    storage_options=asset.extra_fields["xarray:storage_options"],
    **asset.extra_fields["xarray:open_kwargs"],
)

def download_variable_month(year, month, variable):
    
    print(f"(downloading {variable} for {year}-{month:02d})")

    start_date = f"{year}-{month:02d}-01"
    last_day = calendar.monthrange(year, month)[1]
    end_date = f"{year}-{month:02d}-{last_day}"

    # time slice
    ds_slice = ds.sel(time=slice(start_date, end_date))
    ds_variable = ds_slice[[variable]]
    ds_loaded = ds_variable.load()

    # output
    month_dir = os.path.join(OUTPUT_DIR, str(year), f"{month:02d}")
    os.makedirs(month_dir, exist_ok=True)

    # save
    output_file = os.path.join(month_dir, f"{variable}.nc")
    ds_loaded.to_netcdf(output_file)

    print(f"({variable} for {year}-{month:02d} complete)")
    return output_file

tasks = []
for year in YEARS:
    for month in range(1, 13):
        for variable in VARIABLES:
            tasks.append((year, month, variable))

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(download_variable_month, year, month, variable): (year, month, variable) for year, month, variable in tasks}

    for future in as_completed(futures):
        year, month, variable = futures[future]
        try:
            future.result()
        except Exception as exc:
            print(f'ERROR: {variable} for {year}-{month:02d} failed {exc}')
