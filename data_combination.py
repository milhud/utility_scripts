import os
import xarray as xr
import dask

OUTPUT_DIR = "conus404_data"
YEARS = [1988]  # years and vars
VARIABLES = ["T2", "Q2"]  # desired variables

def combine_monthly_data(year, variables, output_dir):
    
    print(f"(dask: combining data for year {year})")
    monthly_datasets = []
    for month in range(1, 13):
        month_dir = os.path.join(output_dir, str(year), f"{month:02d}")
        if not os.path.exists(month_dir):
            print(f"(skipping month {month:02d} for year {year} as directory does not exist)")
            continue

        datasets_for_month = []
        for var in variables:
            file_path = os.path.join(month_dir, f"{var}.nc")
            if os.path.exists(file_path):
                try:
                    # open each file with dask
                    ds = xr.open_dataset(file_path, chunks={'time': -1}) 
                    datasets_for_month.append(ds)
                except Exception as e:
                    print(f"(error opening {file_path}: {e})")
            else:
                print(f"(file not found: {file_path})")

        if datasets_for_month:
            merged_ds_month = xr.merge(datasets_for_month)
            monthly_datasets.append(merged_ds_month)
        else:
             print(f"(no valid datasets found for month {month:02d}, year {year})")

    if monthly_datasets:
        combined_ds = xr.concat(monthly_datasets, dim="time")
        output_filename = os.path.join(output_dir, f"conus404_{year}_combined.nc")

        # Write to netcdf using dask
        combined_ds.to_netcdf(output_filename)
        print(f"Combined data for {year} saved to {output_filename}")
    else:
        print(f"ERROR: no monthly data found to combine for year {year}")



for year in YEARS:
    combine_monthly_data(year, VARIABLES, OUTPUT_DIR)

print("Complete.")
