A refactored version of the [ATLAS datasets-aggregated-regionally](https://github.com/SantanderMetGroup/ATLAS/tree/main/datasets-aggregated-regionally).

```bash
find ATLAS/datasets-aggregated-regionally/data/CMIP6/ -type f | python cmip6.py
```

A CSV dataset can be generated from the netCDF:

```python
import os
import xarray

dst = "CMIP6.csv"
cols = ["model", "experiment", "region", "realm", "time", "tas", "pr"]
with open(dst, "w") as f:
    f.write(",".join(cols))
    f.write("\n")

ds = xarray.open_dataset("CMIP6.nc")
for i in range(ds.dims["model"]):
    for j in range(ds.dims["region"]):
        df = ds.isel(model=slice(i,i+1), region=slice(j,j+1)).to_dataframe().dropna(0).reset_index()
        df[cols].to_csv(dst, header=False, index=None, mode="a")
ds.close()
```

Calculate means:

```python
import pandas as pd

df = pd.read_csv("CMIP6.csv")
means = df.groupby(["model", "experiment", "region", "realm"]).mean()
means.reset_index().to_csv("CMIP6-means.csv", index=None)
```
