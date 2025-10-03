# download_era5_iterative.py
import cdsapi
import os

OUTPUT_DIR = "era5_data"
YEARS = range(1988, 1990)
VARIABLES = ["specific_rain_water_content", "temperature"]

os.makedirs(OUTPUT_DIR, exist_ok=True)
c = cdsapi.Client()
dataset = "reanalysis-era5-pressure-levels"

for year in YEARS:
    for var in VARIABLES:
        filename = os.path.join(OUTPUT_DIR, f"era5_{var}_{year}.nc")
        if os.path.exists(filename):
            continue
        c.retrieve(
            dataset,
            {
                "data_format": "netcdf",
                "product_type": "reanalysis",
                "variable": var,
                "pressure_level": "1000",
                "year": str(year),
                "month": [f"{m:02d}" for m in range(1, 13)],
                "day": [f"{d:02d}" for d in range(1, 32)],
                "time": ["00:00", "06:00", "12:00", "18:00"],
                "area": [50, -126, 24, -65],
            },
            filename,
        )
