# combine_era5_files.py
import os
import xarray as xr

OUTPUT_DIR = "era5_data"
YEARS = range(1988, 1990)
VARIABLES = ["specific_rain_water_content", "temperature"]

combined_datasets = []

for year in YEARS:
    yearly_datasets = []
    for var in VARIABLES:
        file_path = os.path.join(OUTPUT_DIR, f"era5_{var}_{year}.nc")
        if os.path.exists(file_path):
            ds = xr.open_dataset(file_path)
            yearly_datasets.append(ds)
    if yearly_datasets:
        combined_year = xr.merge(yearly_datasets)
        combined_datasets.append(combined_year)

if combined_datasets:
    full_combined = xr.concat(combined_datasets, dim="time")
    output_file = os.path.join(OUTPUT_DIR, "era5_combined.nc")
    full_combined.to_netcdf(output_file)
