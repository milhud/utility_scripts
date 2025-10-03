import os
import xarray as xr
import dask

OUTPUT_DIR = "conus404_data"
YEARS = [1988]
VARIABLES = ["T2", "Q2"]

variable_map = {
    "t": False,
    "specific_rain_water_content": True
}

def temporal_align_datasets(ds_era5, ds_conus, variable_map, target_freq="1D"):
    aligned_results = {}
    for var, is_extensive in variable_map.items():
        if var not in ds_era5.data_vars or var not in ds_conus.data_vars:
            continue
        if is_extensive:
            conus_resampled = ds_conus[var].resample(time=target_freq).sum()
        else:
            conus_resampled = ds_conus[var].resample(time=target_freq).mean()
        era5_aligned, conus_aligned = xr.align(ds_era5[var], conus_resampled, join="inner")
        aligned_results[var] = {"era5": era5_aligned, "conus": conus_aligned}
    return aligned_results

def combine_monthly_data(year, variables, output_dir):
    monthly_datasets = []
    for month in range(1, 13):
        month_dir = os.path.join(output_dir, str(year), f"{month:02d}")
        if not os.path.exists(month_dir):
            continue
        datasets_for_month = []
        for var in variables:
            file_path = os.path.join(month_dir, f"{var}.nc")
            if os.path.exists(file_path):
                ds = xr.open_dataset(file_path, chunks={'time': -1})
                datasets_for_month.append(ds)
        if datasets_for_month:
            monthly_datasets.append(xr.merge(datasets_for_month))
    if not monthly_datasets:
        return
    combined_ds = xr.concat(monthly_datasets, dim="time")
    combined_ds = combined_ds.sel(time=slice(f"{year}-01-01", f"{year}-12-31"))
    combined_ds = combined_ds.rename({"T2": "t"})
    ds_era5 = xr.open_dataset("era5_data.nc").sel(valid_time=slice(f"{year}-01-01", f"{year}-12-31"))
    aligned = temporal_align_datasets(ds_era5, combined_ds, variable_map, target_freq="1D")
    output_filename = os.path.join(output_dir, f"conus404_{year}_combined_daily.nc")
    xr.Dataset({k: v["conus"] for k, v in aligned.items()}).to_netcdf(output_filename)

for year in YEARS:
    combine_monthly_data(year, VARIABLES, OUTPUT_DIR)
