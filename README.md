### **`nyc_supermarkets.csv`**
[download](https://drive.google.com/file/d/1RmxHrY1UNVfAtU8Wut_oqz-G7nsU3Ss-/view?usp=sharing)

This CSV file contains a subset of the grocery stores that we collected through Google Places API. It is a subset because not every store has corresponding POI in the [Safegraph Places dataset](https://docs.safegraph.com/docs/core-places). 

|place_id|latitude|longitude|name|vicinity|rating|user_ratings_total|community_district|percent_food_insecurity|safegraph_placekey|safegraph_name|
|--|--|--|--|--|--|--|--|--|--|--|
|...|...|...|...|...|...|...|...|...|...|...|

In this challenge, we are only interested in the **safegraph_placekey** column which matches the **placekey** column in the Weekly Pattern dataset.


This dataset is considered *small*, and will not be stored on HDFS (when we run our job).

### **`nyc_cbg_centroids.csv`**
[download](https://drive.google.com/file/d/196F50FfY1kHJItadHcU_kQCdViV6puHT/view?usp=sharing)

This CSV file contains the centroid location for each census block group (CBG). This information will be useful in computing the haversine distance between two CBGs. The dataset has the following columns:

|cbg_fips|latitude|longitude|
|--|--|--|
|...|...|...|

**cbg_fips** refers to the FIPS code of the CBGs used in Safegraph, it has the following format:

<img src="https://drive.google.com/uc?id=1io_iPuKOtq0KThPDfpvDAQEd79l6vZVs"></img>

The FIPS code for New York State is **036**. The five boros of New York would have the following FIPS prefix:

|Boro|FIPS Prefix|
|--|--|
|Manhattan|36061...|
|Bronx|36005...
|Brooklyn|36047...|
|Queens|36081...|
|Staten Island|36085...|

This dataset is considered *small*, and will not be stored on HDFS (when we run our job).

### **`weekly-patterns-nyc-2019-2020`**
The data is located under the bucket `gs://bdma/data/weekly-patterns-nyc-2019-2020/part-*`.

This is the *big data* part of the challenge. It is stored on HDFS at the above location. It is a subset of the [Weekly Patterns](https://docs.safegraph.com/docs/weekly-patterns) dataset from Safegraph containing visit patterns for various locations in NYC in 2019 and 2020. We are going to use this data set to derive the traveled distances. Here are the columns of interests:

* **placekey**: can be used to crosscheck with the stores in `nyc_supermarkets.csv`
* **poi_cbg**: the CBG of the store (where people travel to)
* **visitor_home_cbgs**: the list of home CBGs where the visitors were traveling from (yes, we assume they went from home to stores).
* **date_range_end**, **date_range_start**: we use these two date to determine which month the **visitor_home_cbgs** should be used for (more on the pipeline below).

## OUTPUT
You are asked to produce the following output on HDFS in CSV format with the first row of partition 0 (or every partition) containing the column header. Each cell represents the average distance (in miles) that people travel from the corresponding CBG in the month specified in the column. An empty cell suggests that there were no data for that month.

|cbg_fips|2019-03|2019-10|2020-03|2020-10|
|--|--|--|--|--|
|360050147011||0.54|3.21|0.28|
|360050177013|0.22|0.42||0.17|
|360050177023|0.33|0.22|3.45||
|...|...|...|...|...|

The data must be **sorted by `cbg_fips`**.

You must at least run your code once and produce an output under `gs://bdma/shared/2023_spring/FC/EMPLID_LastName` (replacing `EMPLID` and `LastName` with yours).

## APPROACH

To minize the descrepancies, we provide the logic/steps needed to derive the output:

1. Use **`nyc_supermarkets.csv`** to filter the visits in the weekly patterns data
2. Only visit patterns with `date_range_start` or `date_range_end` overlaps with the 4 months of interests (Mar 2019, Oct 2019, Mar 2020, Oct 2020) will be considered, i.e. either the start or the end date falls within the period.
3. Use `visitor_home_cbgs` as the travel origins, and only consider CBG FIPS for NYC (must exist in **`nyc_cbg_centroids.csv`**).
4. Travel distances are the distances between each CBG in `visitor_home_cbgs` with `poi_cbg`. The distance must be computed in miles. To compute the distance between coordinates locally to NYC more accurately, please project them to the EPSG 2263 first.
5. The average should be computed from the total travel distances and the total number of visitors (similar to the way we compute the average in Lab 4).

## SUBMISSION
While you can test using Colab, your final submission must be a single `.py` file that runs on DataProc with the following command

```
!gcloud dataproc jobs submit pyspark --cluster bdma --files nyc_supermarkets.csv,nyc_cbg_centroids.csv \
--properties-file=props.conf BDM_FC_EMPLID_LastName.py -- OUTPUT_FOLDER_NAME
```

and the following configuration file (use `%%writefile props.conf`):
```
spark.hadoop.fs.gs.requester.pays.mode=AUTO
spark.hadoop.fs.gs.requester.pays.project.id=YOUR_PROJECT_ID
spark.executor.instances='4'
spark.executor.cores='4'
```

**REMEMBER** to create your cluster with 4 workers instead of 2 (`--num-workers 4`).

Notes:
* `OUTPUT_FOLDER_NAME`: being specified by the user at run-time. You can access this variable through `sys.argv[1]` in your code. Your code must output data into this folder (e.g. through `saveAsTextFile`).
* As noted above, you need to at least successfully run your code once and output the data to `gs://bdma/shared/2023_spring/FC/EMPLID_LastName` (replacing `EMPLID` and `LastName` with yours).`
* `nyc_supermarkets.csv` and `nyc_cbg_centroids.csv` will be in the same folder with your code when we evaluate your assignment.

##**TIME LIMIT**: your code must not take more than 5 minutes to complete.
