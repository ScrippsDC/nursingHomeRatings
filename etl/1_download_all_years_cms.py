
"""
DWL 10-2023

- Download all statements of deficiciency statements
- Download all Nursing-Home-Compare/Care-Compare datasets


ouput to ../data/source/statements-of-deficiency/
         ../data/source/nursinghome-compare/


"""


##############
# SETTING UP #
###############

import wget
import os.path
from glob import glob
from urllib.parse import urlparse, unquote
from urllib.request import urlretrieve
from zipfile import ZipFile
import pandas as pd
import requests
import re

source = "../data/source/"
processed = "../data/processed/"

def download_sod():
    months = ['January','February','March','April','June','July','August','September','October','November','December']
    years  = [i for i in range(2020,2024)]
    # Example URL: https://downloads.cms.gov/files/Full-Statement-of-Deficiencies-February-2023.zip
    sod_url_stem = "https://downloads.cms.gov/files/"
    sod_file_name = "Full-Statement-of-Deficiencies"
    sod_file_name_lower = sod_file_name.lower()
    months_lower = [m.lower() for m in months]
    for year in years:
        for month in (months):
            for filename in [sod_file_name]:
                filename_full = "{}-{}-{}.zip".format(filename, month, year)
                url_full = "{}{}".format(sod_url_stem, filename_full)
                filepath_full = "../data/source/sod/{}".format(filename_full)
                if os.path.isfile(filepath_full):
                    print(filename_full, "already exists")
                else:
                    try:
                        print("download", url_full)
                        wget.download(url_full, filepath_full)
                        #print()
                    except:
                        print("no such file {}".format(filename_full))



def extract_sod():
    filepath = source + "sod/"
    filespec_zip  = filepath + "*.zip"
    for zipname in glob(filespec_zip):
        with ZipFile(zipname) as myzip:
            print("extracting {}".format(zipname))
            myzip.extractall(filepath)
            prep_sod(zipname)
        os.remove(zipname)

def prep_sod(zipname="none.zip"):
    filepath = source + "sod/"
    filespec_xlsx = filepath + "*.xlsx"
    dataframes = []
    for xlsx in glob(filespec_xlsx):
        print("reading {} ...".format(xlsx))
        try:
            dataframes.append(pd.read_excel(xlsx))
        except:
            print("error reading {}".format(xlsx))
        os.remove(xlsx)
    df = pd.concat(dataframes)
    savename = zipname.replace(".zip", ".parquet")
    print("saving {} ...".format(savename),)
    df.to_parquet(savename, compression='brotli')
    print("done")


def download_nursinghome_compare():
    """
    Loop throughg the annual downloads since 2015.

    Uses "pagefreezer" for pre-2023
    """
    
    filepath = source + "nursinghome-compare/"

    urls = [
           "https://public4.pagefreezer.com/content/Data.CMS.gov%20Provider%20Data/02-11-2022T14:37/https://data.cms.gov/provider-data/sites/default/files/archive/Nursing%20homes%20including%20rehab%20services/2015/nh_archive_2015.zip",
           "https://public4.pagefreezer.com/content/Data.CMS.gov%20Provider%20Data/02-11-2022T14:37/https://data.cms.gov/provider-data/sites/default/files/archive/Nursing%20homes%20including%20rehab%20services/2016/nh_archive_2016.zip",
            "https://data.cms.gov/provider-data/sites/default/files/archive/Nursing%20homes%20including%20rehab%20services/2017/nh_archive_2017.zip",
           "https://data.cms.gov/provider-data/sites/default/files/archive/Nursing%20homes%20including%20rehab%20services/2018/nh_archive_2018.zip",
           "https://data.cms.gov/provider-data/sites/default/files/archive/Nursing%20homes%20including%20rehab%20services/2019/nursing_homes_including_rehab_services_2019.zip",
           "https://data.cms.gov/provider-data/sites/default/files/archive/Nursing%20homes%20including%20rehab%20services/2020/nursing_homes_including_rehab_services_2020.zip",
           "https://data.cms.gov/provider-data/sites/default/files/archive/Nursing%20homes%20including%20rehab%20services/2021/nursing_homes_including_rehab_services_2021.zip",
           "https://data.cms.gov/provider-data/sites/default/files/archive/Nursing%20homes%20including%20rehab%20services/2022/nursing_homes_including_rehab_services_2022.zip",
           "https://data.cms.gov/provider-data/sites/default/files/archive/Nursing%20homes%20including%20rehab%20services/2023/nursing_homes_including_rehab_services_2023.zip",
            "https://data.cms.gov/provider-data/sites/default/files/archive/Nursing%20homes%20including%20rehab%20services/2024/nursing_homes_including_rehab_services_2024.zip"]
    print("downloading annual files")
    for url in urls:
        filename = filepath +  os.path.basename(urlparse(url).path)
        with open(filename, "wb+") as file:
            print("downloading {} ... ".format(url))
            r = requests.get(url)
            print("saving as {} ...".format(filename))
            file.write(r.content)
            


def extract_nursinghome_compare():
    filepath = source + "nursinghome-compare/"
    filespec_zip  = filepath + "*.zip"
    for zipname in glob(filespec_zip):
        with ZipFile(zipname) as myzip:
            print("extracting {}".format(zipname))
            year = zipname[-8:-4] # using array slicing
            os.makedirs(filepath + year, exist_ok=True)
            myzip.extractall(filepath + year )
            os.remove(zipname)
            if int(year) <= 2016:
                prep_nursinghome_compare_single(year)
            else:
                prep_nursinghome_compare(year)


def prep_nursinghome_compare(year):
    filepath = source + "nursinghome-compare/{}/".format(year)
    filespec_zip  = filepath + "*.zip"
    print("preparing zipfiles for {}".format(year))
    for zipname in glob(filespec_zip):
        month = zipname[-11:-9]
        #print("mmmmmm", month)
        #print("zzzzzzz", zipname)
        outpath = filepath + month
        os.makedirs(outpath, exist_ok=True)
        #print("****", outpath)
        with ZipFile(zipname) as myzip:
            #print("extracting {}".format(zipname))
            myzip.extractall(outpath)
        prep_nursinghome_compare_multi(year, month)
        os.remove(zipname)

def prep_nursinghome_compare_multi(year, month):
    filepath = source + "nursinghome-compare/" + year + "/" + month + "/"
    filespec_csv = filepath + "*.csv"
    outpath = filepath
    #print("ooooo", outpath)
    os.makedirs(outpath, exist_ok=True)
    previous_csv = ""
    previous_outfle = ""
    previous_df = pd.DataFrame()
    csvs = glob(filespec_csv)
    csvs.sort()
    #print(csvs)
    for csv in csvs:
        #print(csv)
        try:
            df = pd.read_csv(csv, dtype='str', encoding='windows-1252')
        except:
            print("Error reading", csv)
            break
        os.remove(csv) 
        outfile = csv.replace(".csv", ".parquet").replace(filepath, outpath)
        if csv.find("_to_") > -1 and previous_csv.find("_to_") > -1:
            df = pd.concat((previous_df, df))
            try:
                os.remove(previous_outfile)
            except:
                pass
            previous_csv = csv
            outfile = outfile.split("_to_")[0] + "_" + year + ".parquet"
        else:
            previous_csv = csv
        #print("csv name ", csv)
        previous_df = df
        previous_outfile = outfile
        df.to_parquet(outfile, compression="snappy") # duckdb doesn't accept brotli

def prep_nursinghome_compare_single(year, month="12"):
    print("preparing csvs for  {} zipfile".format(year))
    filepath = source + "nursinghome-compare/" + year + "/"
    outpath = filepath + month + "/"
    #print("ooooo", outpath)
    filespec_csv  = filepath + "*.csv"
    os.makedirs(outpath, exist_ok=True)
    previous_csv = ""
    previous_outfle = ""
    previous_df = pd.DataFrame()
    csvs = glob(filespec_csv)
    csvs.sort()
    for csv in csvs:
        #print(csv)
        df = pd.read_csv(csv, dtype={'provnum':'str', 'PROVNUM':'str'}, encoding='windows-1252')
        os.remove(csv) 
        outfile = csv.replace(".csv", ".parquet").replace(filepath, outpath)
        if csv.find("_to_") > -1 and previous_csv.find("_to_") > -1:
            df = pd.concat((previous_df, df))
            try:
                os.remove(previous_outfile)
            except:
                pass
            previous_csv = csv
            outfile = outfile.split("_to_")[0] + "_" + year + ".parquet"
        else:
            previous_csv = csv
        #print("csv name ", csv)
        previous_df = df
        previous_outfile = outfile
        df.to_parquet(outfile, compression="brotli")


if __name__ == "__main__":
    # download_sod()
    # extract_sod()
    download_nursinghome_compare()
    extract_nursinghome_compare()
