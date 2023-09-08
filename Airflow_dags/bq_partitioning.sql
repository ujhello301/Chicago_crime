CREATE OR REPLACE TABLE `dtc-de-course-375420.chicago_crime_all.ET_CHICAGO_CRIMES_DATEFORMATTED` AS
SELECT
  DATE(PARSE_DATETIME("%m/%d/%Y %I:%M:%S %p", date)) AS New_Date,
  DATE_TRUNC(DATE(PARSE_DATETIME("%m/%d/%Y %I:%M:%S %p", date)), MONTH) AS Month,*
FROM
  {{ params.projectId }}.{{ params.datasetId }}.{{ params.tableId }};
  
  
CREATE OR REPLACE TABLE `dtc-de-course-375420.chicago_crime_all.CHICAGO_CRIMES_PARTITIONED`
PARTITION BY Month
CLUSTER BY PRIMARY_TYPE
AS
SELECT * FROM `dtc-de-course-375420.chicago_crime_all.ET_CHICAGO_CRIMES_DATEFORMATTED`;