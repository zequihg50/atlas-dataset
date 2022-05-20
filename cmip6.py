import sys
import re
import numpy as np
import pandas as pd
import netCDF4

DATASET = "CMIP6.nc"
FILL = 9999.
MODELS = {
    'ACCESS-CM2': 0, 'ACCESS-ESM1-5': 1, 'AWI-CM-1-1-MR': 2, 'BCC-CSM2-MR': 3, 'CAMS-CSM1-0': 4, 'CanESM5': 5,
    'CESM2': 6, 'CESM2-WACCM': 7, 'CMCC-CM2-SR5': 8, 'CNRM-CM6-1': 9, 'CNRM-CM6-1-HR': 10, 'CNRM-ESM2-1': 11,
    'EC-Earth3': 12, 'EC-Earth3-Veg': 13, 'EC-Earth3-Veg-LR': 14, 'FGOALS-g3': 15, 'GFDL-CM4': 16, 'GFDL-ESM4': 17,
    'HadGEM3-GC31-LL': 18, 'IITM-ESM': 19, 'INM-CM4-8': 20, 'INM-CM5-0': 21, 'IPSL-CM6A-LR': 22, 'KACE-1-0-G': 23,
    'KIOST-ESM': 24, 'MIROC6': 25, 'MIROC-ES2L': 26, 'MPI-ESM1-2-HR': 27, 'MPI-ESM1-2-LR': 28, 'MRI-ESM2-0': 29,
    'NESM3': 30, 'NorESM2-LM': 31, 'NorESM2-MM': 32, 'TaiESM1': 33, 'UKESM1-0-LL': 34
}
# find datasets-aggregated-regionally/data/CMIP6/ -type f | while read f ; do awk -F, 'NR==16{for(i=2; i<=NF; i++){print $i}}' ${f} ; done | sort -u | awk '{printf("%s: %s,\n", $0, NR)}'
REGIONS = {
    'world': 0, 'ARO': 1, 'ARP': 2, 'ARS': 3, 'BOB': 4, 'CAF': 5, 'CAR': 6, 'CAU': 7, 'CNA': 8, 'EAN': 9, 'EAO': 10, 'EAS': 11,
    'EAU': 12, 'ECA': 13, 'EEU': 14, 'EIO': 15, 'ENA': 16, 'EPO': 17, 'ESAF': 18, 'ESB': 19, 'GIC': 20, 'MDG': 21, 'MED': 22,
    'NAO': 23, 'NAU': 24, 'NCA': 25, 'NEAF': 26, 'NEN': 27, 'NES': 28, 'NEU': 29, 'NPO': 30, 'NSA': 31, 'NWN': 32, 'NWS': 33,
    'NZ': 34, 'RAR': 35, 'RFE': 36, 'SAH': 37, 'SAM': 38, 'SAO': 39, 'SAS': 40, 'SAU': 41, 'SCA': 42, 'SEA': 43, 'SEAF': 44,
    'SES': 45, 'SIO': 46, 'SOO': 47, 'SPO': 48, 'SSA': 49, 'SWS': 50, 'TIB': 51, 'WAF': 52, 'WAN': 53, 'WCA': 54, 'WCE': 55,
    'WNA': 56, 'WSAF': 57, 'WSB': 58
}
EXPERIMENTS = {'historical': 0, 'ssp126': 1, 'ssp245': 2, 'ssp370': 3, 'ssp585': 4}
REALMS = {'land': 0, 'sea': 1, 'landsea': 2}
DRS = ".*/[^_]+_(?P<DRS_variable>[^_]+)_(?P<DRS_realm>[^_]+)/(?P<DRS_project>[^_]+)_(?P<DRS_model>[^_]+)_(?P<DRS_experiment>[^_]+)_(?P<DRS_ensemble>[^_]+)\.csv"

def parse_drs(fname):
    p = re.compile(DRS)
    matches = p.search(fname)

    return matches.groupdict()

def parse_header(fname):
    header = {}
    for i,line in enumerate(open(fname, "r")):
        parts = line.rstrip("\n").split(": ")
        k = parts[0].lstrip("#")
        header[k] = ": ".join(parts[1:])
        if i >=14: # end of header
            break

    return header

def setup_nc(name, timesteps):
    nc = netCDF4.Dataset(name, "w")
    nc.createDimension("model", len(MODELS))
    nc.createDimension("experiment", len(EXPERIMENTS))
    nc.createDimension("region", len(REGIONS))
    nc.createDimension("realm", len(REALMS))
    nc.createDimension("time", len(timesteps))

    string_dtype = "S50"
    nc.createVariable("model", string_dtype, ("model",))
    nc.createVariable("experiment", string_dtype, ("experiment",))
    nc.createVariable("region", string_dtype, ("region",))
    nc.createVariable("realm", string_dtype, ("realm",))
    nc.createVariable("time", string_dtype, ("time",))

    nc.variables["model"][:] = np.array(list(MODELS.keys()), string_dtype)
    nc.variables["experiment"][:] = np.array(list(EXPERIMENTS.keys()), string_dtype)
    nc.variables["region"][:] = np.array(list(REGIONS.keys()), string_dtype)
    nc.variables["realm"][:] = np.array(list(REALMS.keys()), string_dtype)
    nc.variables["time"][:] = np.array(list(timesteps.keys()), string_dtype)

    for v in ["tas", "pr"]:
        nc.createVariable(
            v,
            "f4",
            ("model", "experiment", "region", "realm", "time"),
            zlib=True,
            shuffle=True,
            fletcher32=True,
            fill_value=FILL,
            chunksizes=(1, 1, len(REGIONS), 1, 12*50)) # 12 months * 20 years
        for region in REGIONS:
            for model in MODELS:
                nc.variables[v][MODELS[model], :, REGIONS[region], :, :] = FILL

    nc.close()

if __name__ == "__main__":
    headers = []
    dfs = []

    # timesteps
    timesteps = {}
    counter = 0
    for i in range(1850,2101):
        for j in range(1,13):
            if j < 10:
                day = "0" + str(j)
            else:
                day = str(j)
            k = "-".join([str(i), day])
            timesteps[k] = counter
            counter += 1

    # setup nc
    setup_nc(DATASET, timesteps)

    # store
    nc = netCDF4.Dataset(DATASET, "a")
    for line in sys.stdin:
        fname = line.rstrip("\n")
        drs = parse_drs(fname)
        header = parse_header(fname)
        df = pd.read_csv(fname, skiprows=15)
        df = df.melt(id_vars="date", var_name="region", value_name="value")

        # set dimensions
        df["experiment"] = drs["DRS_experiment"]
        df["model"] = drs["DRS_model"]
        df["variable"] = drs["DRS_variable"]
        df["realm"] = drs["DRS_realm"]

        # substitute, date region value experiment model variable realm
        df["date"] = df["date"].map(timesteps)
        df["region"] = df["region"].map(REGIONS)
        df["model"] = df["model"].map(MODELS)
        df["experiment"] = df["experiment"].map(EXPERIMENTS)
        df["realm"] = df["realm"].map(REALMS)

        # store
        varname = drs["DRS_variable"]
        idate = list(df["date"].unique())
        iregion = list(df["region"].unique())
        values = df["value"].values.reshape((
            len(df["date"].unique()),
            len(df["region"].unique())))
        nc.variables[varname][MODELS[drs["DRS_model"]], EXPERIMENTS[drs["DRS_experiment"]], iregion, REALMS[drs["DRS_realm"]], idate] = values
    nc.close()
    print(DATASET)
