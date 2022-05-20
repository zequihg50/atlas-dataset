A refactored version of the [ATLAS datasets-aggregated-regionally](https://github.com/SantanderMetGroup/ATLAS/tree/main/datasets-aggregated-regionally).

```bash
find ATLAS/datasets-aggregated-regionally/data/CMIP6/ -type f | python cmip6.py
```

A CSV dataset can be generated from the netCDF:

```python
import os
import xarray

dst = "CMIP6.csv"
os.remove(dst)

ds = xarray.open_dataset("CMIP6.nc")
for i in range(ds.dims["model"]):
    for j in range(ds.dims["region"]):
        ds.isel(model=slice(i,i+1), region=slice(j,j+1)).to_dataframe().reset_index().to_csv(dst, header=False, index=None, mode="a")
ds.close()
```
