
## This is the homework project in my big data analysis class.
## all the code is done by myself, and it is correct.
## my code was writen on google colab.

We are greatly inspired by the [Consumer Complaints](https://github.com/InsightDataScience/consumer_complaints) challenge from [InsightDataScience](https://github.com/InsightDataScience/). In fact, we are going to tackle the same challenge but using Apache Spark. Please read through the challenge at the following link:

<https://github.com/InsightDataScience/consumer_complaints>

The most important sections are **Input dataset** and **Expected output**, which are quoted below:

## Input dataset
For this challenge, when we grade your submission, an input file, `complaints.csv`, will be moved to the top-most `input` directory of your repository. Your code must read that input file, process it and write the results to an output file, `report.csv` that your code must place in the top-most `output` directory of your repository.

Below are the contents of an example `complaints.csv` file: 
```
Date received,Product,Sub-product,Issue,Sub-issue,Consumer complaint narrative,Company public response,Company,State,ZIP code,Tags,Consumer consent provided?,Submitted via,Date sent to company,Company response to consumer,Timely response?,Consumer disputed?,Complaint ID
2019-09-24,Debt collection,I do not know,Attempts to collect debt not owed,Debt is not yours,"transworld systems inc. is trying to collect a debt that is not mine, not owed and is inaccurate.",,TRANSWORLD SYSTEMS INC,FL,335XX,,Consent provided,Web,2019-09-24,Closed with explanation,Yes,N/A,3384392
2019-09-19,"Credit reporting, credit repair services, or other personal consumer reports",Credit reporting,Incorrect information on your report,Information belongs to someone else,,Company has responded to the consumer and the CFPB and chooses not to provide a public response,Experian Information Solutions Inc.,PA,15206,,Consent not provided,Web,2019-09-20,Closed with non-monetary relief,Yes,N/A,3379500
2020-01-06,"Credit reporting, credit repair services, or other personal consumer reports",Credit reporting,Incorrect information on your report,Information belongs to someone else,,,Experian Information Solutions Inc.,CA,92532,,N/A,Email,2020-01-06,In progress,Yes,N/A,3486776
2019-10-24,"Credit reporting, credit repair services, or other personal consumer reports",Credit reporting,Incorrect information on your report,Information belongs to someone else,,Company has responded to the consumer and the CFPB and chooses not to provide a public response,"TRANSUNION INTERMEDIATE HOLDINGS, INC.",CA,925XX,,Other,Web,2019-10-24,Closed with explanation,Yes,N/A,3416481
2019-11-20,"Credit reporting, credit repair services, or other personal consumer reports",Credit reporting,Incorrect information on your report,Account information incorrect,I would like the credit bureau to correct my XXXX XXXX XXXX XXXX balance. My correct balance is XXXX,Company has responded to the consumer and the CFPB and chooses not to provide a public response,"TRANSUNION INTERMEDIATE HOLDINGS, INC.",TX,77004,,Consent provided,Web,2019-11-20,Closed with explanation,Yes,N/A,3444592
```
Each line of the input file, except for the first-line header, represents one complaint. Consult the [Consumer Finance Protection Bureau's technical documentation](https://cfpb.github.io/api/ccdb/fields.html) for a description of each field.  

* Notice that complaints were not listed in chronological order
* In 2019, there was a complaint against `TRANSWORLD SYSTEMS INC` for `Debt collection` 
* Also in 2019, `Experian Information Solutions Inc.` received one complaint for `Credit reporting, credit repair services, or other personal consumer reports` while `TRANSUNION INTERMEDIATE HOLDINGS, INC.` received two
* In 2020, `Experian Information Solutions Inc.` received a complaint for `Credit reporting, credit repair services, or other personal consumer reports`

In summary that means 
* In 2019, there was one complaint for `Debt collection`, and 100% of it went to one company 
* Also in 2019, three complaints against two companies were received for `Credit reporting, credit repair services, or other personal consumer reports` and 2/3rd of them (or 67% if we rounded the percentage to the nearest whole number) were against one company (TRANSUNION INTERMEDIATE HOLDINGS, INC.)
* In 2020, only one complaint was received for `Credit reporting, credit repair services, or other personal consumer reports`, and so the highest percentage received by one company would be 100%

For this challenge, we want for each product and year that complaints were received, the total number of complaints, number of companies receiving a complaint and the highest percentage of complaints directed at a single company.

For the purposes of this challenge, all names, including company and product, should be treated as case insensitive. For example, "Acme", "ACME", and "acme" would represent the same company.

## Expected output

After reading and processing the input file, your code should create an output file, `report.csv`, with as many lines as unique pairs of product and year (of `Date received`) in the input file. 

Each line in the output file should list the following fields in the following order:
* product (name should be written in all lowercase)
* year
* total number of complaints received for that product and year
* total number of companies receiving at least one complaint for that product and year
* highest percentage (rounded to the nearest whole number) of total complaints filed against one company for that product and year. Use standard rounding conventions (i.e., Any percentage between 0.5% and 1%, inclusive, should round to 1% and anything less than 0.5% should round to 0%)

The lines in the output file should be sorted by product (alphabetically) and year (ascending)

Given the above `complaints.csv` input file, we'd expect an output file, `report.csv`, in the following format
```
"credit reporting, credit repair services, or other personal consumer reports",2019,3,2,67
"credit reporting, credit repair services, or other personal consumer reports",2020,1,1,100
debt collection,2019,1,1,100
```
Notice that because `debt collection` was only listed for 2019 and not 2020, the output file only has a single entry for debt collection. Also, notice that when a product has a comma (`,`) in the name, the name should be enclosed by double quotation marks (`"`). Finally, notice that percentages are listed as numbers and do not have `%` in them.

# Objectives

In this homework, we will tackle the above problem in two steps (2 tasks):

1. In Task 1, we work on a solution with PySpark on Google Colab using a sample of the data. The data is available on Google Drive and is to be downloaded by the `gdown` command in Task 1.

2. In Task 2, we create a standalone Python script that work on the full dataset using GCP DataProc. The full dataset is downloaded from [here](https://www.consumerfinance.gov/data-research/consumer-complaints/#download-the-data). The data is available on the class bucket as: `gs://bdma/data/complaints.csv`

