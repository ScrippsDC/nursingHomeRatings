# Nursing Home Ratings 

This repository contains data and code to reproduce the data findings in [High ratings for nursing homes may not give full story about care problems, deaths](https://www.scrippsnews.com/investigations/high-ratings-for-nursing-homes-may-not-give-full-story-about-care-problems-deaths) by [Daniel Lathrop](https://github.com/lathropd), [Amy Fan](https://github.com/amyafan), Lori Jane Gliha, and Brittany Freeman, published on May 15, 2024. This investigation focuses on a particular nursing home that held onto a 5-star CMS rating, despite a client dying after the home put her in "immediate jeopardy."

> OUR SCRIPPS NEWS ANALYSIS OF GOVERNMENT DATA FROM FEBRUARY 2024 FOUND 247 NURSING HOMES ACROSS THE UNITED STATES...IN SIMILAR SITUATIONS -- HOLDING TOP OVERALL RATINGS -- A FOUR OR A FIVE - DESPITE "IMMEDIATE JEOPARDY” FINDINGS THAT RESIDENTS HAD BEEN PLACED AT RISK IN THE LAST THREE YEARS -- SINCE FEBRUARY 2021.

> OUR INVESTIGATION FOUND THE FACILITY STILL HELD ON TO ITS TOP OVERALL RATING...5 STARS....FOR SEVEN MONTHS AFTER KAREN DIED

We also mention two other nursing homes in the story that have held onto top ratings after an immediate jeopardy finding.

> A SCRIPPS NEWS ANALYSIS FOUND 397 FOUR AND FIVE STAR NURSING HOMES ACROSS THE COUNTRY COULD POTENTIALLY HAVE JUST ONE STAR BY ANOTHER STATE’S STANDARDS.

## Data Sources
### Nursing homes including rehab services archived data snapshots
[https://data.cms.gov/provider-data/archived-data/nursing-homes](https://data.cms.gov/provider-data/archived-data/nursing-homes)

These are monthly snapshots of the datasets CMS maintains of nursing home and rehab services, including their health rating and their overall rating.

Each snapshot is a zipfile that mostly contains CSVs. In ETL, we resave these as parquet files for easier storage and access. The files relevant to our analysis are:

* Provider information
* Health deficiencies
* State-level health inspection cut points

#### Provider information
From [https://data.cms.gov/provider-data/dataset/4pq5-n9py](https://data.cms.gov/provider-data/dataset/4pq5-n9py):

>"General information on currently active nursing homes, including number of certified beds, quality measure scores, staffing and other information used in the Five-Star Rating System. Data are presented as one row per nursing home."

By combining every available provider information file in the snapshots, we are able to track overall rating and health inspection rating over time -- NH_ProviderInfo_{month}{year}.csv (resaved as parquet files.)

In these filenames, month is formatted as a three-letter abbreviation with the first letter capitalized ("Feb", "Jun", etc), and year is formatted as the full four-digit year.

More complete documentation is available on the [CMS website](https://data.cms.gov/provider-data/sites/default/files/data_dictionaries/nursing_home/NH_Data_Dictionary.pdf)

#### Health deficiencies
From [https://data.cms.gov/provider-data/dataset/r5ix-sfxw](https://data.cms.gov/provider-data/dataset/r5ix-sfxw):

>A list of nursing home health citations in the last three years, including the nursing home that received the citation, the associated inspection date, citation tag number and description, scope and severity, the current status of the citation and the correction date. Data are presented as one citation per row.

Our analysis of nursing homes with immediate jeopardy findings uses the snapshot of health deficiencies from February of 2024 -- NH_HealthCitations_Feb2024.csv (resaved as a parquet file).

Every citation is given a code for "scope severity", lettered A through L. Scope severity codes J, K, and L constitute findings of "immediate jeopardy" (IJ). Immediate Jeopardy _"represents a situation in which entity noncompliance has placed the health and safety of recipients in its care at risk for serious injury, serious harm, serious impairment or death."_

The code J is assigned for isolated cases of immediate jeopardy, K is for a pattern, and L is for if those cases are widespread.

[Scope and Severity Grid](https://www.vdh.virginia.gov/content/uploads/sites/96/2018/12/SS-Grid-with-Description-12-2018.pdf)

#### State-level health inspection cut points
From [https://data.cms.gov/provider-data/dataset/hicp-9999](https://data.cms.gov/provider-data/dataset/hicp-9999):

>State-specific ranges for the weighted health inspection score for each health inspection star rating category. Data are presented as one row per state or territory.

Using the state health inspection citations and scope and severity codes, CMS calculates a raw health inspection score. Citations with a higher scope and severity are assigned more points, and recent citations are weighted more heavily. 

CMS then uses those raw scores to assign each provider with 1 through 5 stars to indicate whether it was one of the worst in the state (1-star) or best in the state (5-star). These cut points are the ranges of raw scores that correspond to each health inspection star rating in each state.

CMS uses different cut points for each state because state inspection practices vary enough that raw scores aren't aligned state-to-state. But the cut points vary widely.

Our analysis uses the snapshot of cut points from February of 2024. -- NH_HlthInspecCutpointsState_Feb2024.csv (resaved as a parquet file). Note that these cut points are only used to calculate the health inspection star rating, which is different (but related to) the overall rating.

## ETL

### Download all years of provider-level ratings data
The code for this step is in [etl/1_download_all_years_cms.py](etl/1_download_all_years_cms.py)

We unzipped all of the available snapshots and resaved all csv files as parquet files for easier storage and access, keeping their original filename. We saved them in the folder `data/source/nursinghome-compare/` and then in subdirectories by four digit year and two digit month `{YYYY}/{MM}`.

### Create a SQL macro to query a nursing home over time

To combine our provider information snapshots into a single result across years, we use a shell script to generate the SQL queries to be used in a macro. This avoids creating a new data table with multiple years and allows more flexible querying. One reason this is needed is that the data format changed in May 2023 and we wanted to directly query the raw data wherever possible.

**Earlier files**
\
XXX: \"Federal Provider Number\", \"Provider Name\", \"Provider State\", \"Overall Rating\", \"Health Inspection Rating\", \"Processing Date\"
YYY: \"Federal Provider Number\"

**Newer files**
\
XXX:  \"CMS Certification Number (CCN)\", \"Provider Name\", \"State\", \"Overall Rating\", \"Health Inspection Rating\", \"Processing Date\"
YYYY:  \"CMS Certification Number (CCN)\"

The SQL statements used with the macro are generated using `find` command in the shell. 

[etl/2_ratings_sql_macro.sql](etl/2_ratings_sql_macro.sql) was constructed by running the following in `data/source/nursinghome-compare` directory:

```bash
find "." | grep NH_ProviderInfo | sort  | xargs printf "select \"Federal Provider Number\", \"Provider Name\", \"Provider State\", \"Overall Rating\", \"Health Inspection Rating\", \"Processing Date\" from '%s' where \"Federal Provider Number\" like 'NNN';\n"

find "." | grep NH_ProviderInfo | sort  | xargs printf "select  \"CMS Certification Number (CCN)\",\"Provider Name\", \"State\", \"Overall Rating\", \"Health Inspection Rating\", \"Processing Date\" from '%s' where \"CMS Certification Number (CCN)\" like 'NNN';\n"
```

We then joined the SELECT statements with UNION ALL into a script defining a macro "ratings_by_provider_over_time", which takes provider number as an argument, and we fix the path names to be runnable from our notebook.

We run this sql script from within our analysis notebook: [analysis/final_analysis_notebook.ipynb](analysis/final_analysis_notebook.ipynb)

## Analysis

The analysis for this piece is in the notebook [analysis/final_analysis_notebook.ipynb](analysis/final_analysis_notebook.ipynb).

To find the number of 4 and 5 star facilities with an immediate jeopardy finding on their record in February, 2024, we:

1. Filter the *health deficiencies* table down to citations after January 31st, 2021 and immediate jeopardy citations ("Scope Severity Code" in ["J","K","L"]), and drop duplicate "CMS Certification Numbers (CCN)" -- **facilities_with_ij**
2. Inner join the *provider information table* (NH_ProviderInfo_Feb2024.parquet) to **immediate_jeopardy_citations**
3. Filter for providers with an "Overall Rating" greater than or equal to 4. (247 providers)

To track the overall ratings over time for Touchmark on South Hill, Willowbrooke Court, and Good Samaritan Society over time, we identify them by their provider number ("CMS Certification Number (CCN)" for files in June 2023 and afterwards and "Federal Provider Number" for files from August 2020 to May 2023) and generate a "ratings_by_provider_over_time" table for each of them.

To count the number of 4 or 5 star homes that would have 1 star in another state, based on their numeric Health Inspection Score, we find the lowest cut-points for every tier and save them to a table. The state with the lowest 1-star cut point is Alabama, at 33.667. -- **cutoffs**

We then create the field "worst_inspection", which recalculates health inspection ratings for every home in **ratings_and_scores** (loaded from NH_ProviderInfo_Feb2024.parquet), using the cut points in **cutoffs**. Relying on the Overall Star Rating methodology published [here](https://www.cms.gov/medicare/provider-enrollment-and-certification/certificationandcomplianc/downloads/usersguide.pdf) we recalculate overall star ratings using "worst_inspection", to create the field "worst_rating".

>Step 1: Start with the health inspection rating.
>Step 2: Add one star to the Step 1 result if the staffing rating is five stars; subtract one star if the staffing rating is one star. The overall rating cannot be more than five stars or less than one star.
>Step 3: Add one star to the Step 2 result if the quality measure rating is five stars; subtract one star if the quality measure rating is one star. The overall rating cannot be more than five stars or less than one star.

>Note: If the health inspection rating is one star, then the overall rating cannot be upgraded by more than one star based on the staffing and quality measure ratings. 

Then we count the number of homes in with an "Overall Rating" of 4 or 5 and a "worst_rating" of 1 or lower. (As noted above, all rating scores calculated to be lower than 1 are rounded up to "1".) (397 providers)