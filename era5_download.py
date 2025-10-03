c = cdsapi.Client()
dataset = 'reanalysis-era5-pressure-levels'

c.retrieve(
    dataset,
    {
        'data_format': 'netcdf',
        'product_type': ['reanalysis'],
        'variable': ['specific_rain_water_content','temperature'],
        'pressure_level': '1000',
        'year': '1988',
        'month': [f'{m:02d}' for m in range(1, 13)],
        'day': [f'{d:02d}' for d in range(1, 32)],
        'time': ['00:00', '06:00', '12:00', '18:00'],
        "area": [50, -126, 24, -65],
    },
    'era5_1hpa_conus_1988.nc'
)

ds_era5 = xr.open_dataset('era5_1hpa_conus_1988.nc')
print(ds_era5)
