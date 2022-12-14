# jordan-etl
Dependencies:
- SQL Alchemy 1.4.39
- Scrapy      2.6.2
- cx-Oracle   8.3.0
- Pandas      1.4.3

1) In **./parsers/spiders/database** find a **connector.py** file and set up your DB credentials.
2) From **./parsers/spiders/database** run **database.py** file to create a staging database.
3) As for now in **./parsers/spiders**, there are 5 python-based independent parsers to collect data for 5 indicators:
    1) **interest_rate.py**        -> source updated daily
    2) **consumer_price_index.py** -> source updated monthly
    3) **inflation_rate.py**       -> source updated monthly
    4) **housing_index.py**        -> source updated quarterly
    5) **unemployment_rate.py**    -> source updated quarterly

    In **./parsers/spiders** there is also a sample (**example.py**) spider which can be used to extract data from any web-page 
  by means of replacing the initial **start_urls** to a desirable web-page address and **elements_to_parse** to a desirable html-elements to retrieve.
  The spider will return a .txt file with converted to text enumerated html-elements.
  
    Apart from that, in **./parsers/spiders** you can find a **csv_loader.py**. It was created on the basis of the provided .CSV samples and represents a one-time
  script to load those .CSVs to Oracle. Before running this script - add a path to the file you want to import.

    All the scripts are built to be executed as a usual .py files (from IDE or from terminal). 
   _E.g.: python consumer_price_index.py && python inflation_rate.py_ 
    
    All 5 parsers write data to STG_INDICATORS table (created with SQL Alchemy ORM) which is a staging string-based table.
4) Data transformation scripts, located in **./scripts** are written as Oracle objects: triggers and stored procedures. 
5) Create tables from the **tables.sql** file.
6) **transformation.sql** file contains transformation scripts which has to be created and then executed sequentially:
7) **spr_MergeDimIndicators** has to be executed first. It will merge the data extracted and loaded with parsers into DIM_INDICATORS & FACT_INDICATOR tables.
8) **spr_InsertDimOld** has to be executed after **spr_MergeDimIndicators**. It will return the historical records back to the DIM_INDICATORS.
