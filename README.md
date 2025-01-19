# BCG_assignment

Source Data:
  - "Restrict_use.csv" : Contains information about driver's license restrictions for individuals involved in crashes.

  - "Charges_use.csv": Lists charges filed against drivers involved in crashes.

  - "Units_use.csv": Provides details about vehicles involved in crashes.
      
  - "Primary_Person_use.csv": Contains information about individuals involved in crashes, primarily focusing on drivers.
    
  - "Damages_use.csv":  Records properties damaged during crashes.
  
  -"Endorse_use.csv":  Details endorsements or restrictions on driver's licenses for individuals involved in crashes.

config.yml: This script is responsible for storing the source path of all the data files for ETL scripts.

main_functions: main.py will trigger all the analysis functions from analysis.py.

analysis_functions:
  analysis_1:  Counts crashes where more than two males were killed. 

  analysis_2: Counts the number of motorcycles booked for crashes. 
    assumptions:
      - The vehicle body style "Motorcycle" is the only key in the dataset for two wheeler.

  analysis_3:  Identifies the top five vehicle makes where drivers died and airbags did not deploy. 
    assumptions:
      - Assuming Joining Key with different source files are "CRASH_ID","UNIT_NBR". Comibining these two keys to make the record distinct.

  analysis_4: Counts distinct vehicles with valid licenses involved in hit-and-run incidents. 
    assumptions:
      - Assuming Joining Key with different source files are "CRASH_ID","UNIT_NBR". Comibining these two keys to make the record distinct.
      - We are including driver of two-wheeler and four-wheeler in this analysis.

  analysis_5:  Identifies which state had the highest number of accidents without females involved. 
    assumptions:
      - We are taking two values apart from FEMALE.

  analysis_6:  Identifies vehicle makes contributing to injuries excluding non-injured individuals. 
    assumptions:
      - Assuming Joining Key with different source files are "CRASH_ID","UNIT_NBR". Comibining these two keys to make the record distinct.

  analysis_7: Determines the top ethnic user groups associated with unique body styles. 
    assumptions:
      - Assuming Joining Key with different source files are "CRASH_ID","UNIT_NBR". Comibining these two keys to make the record distinct.

  analysis_8: Identifies zip codes associated with alcohol-related crashes. 
    assumptions:
      - Assuming Joining Key with different source files are "CRASH_ID","UNIT_NBR". Comibining these two keys to make the record distinct.

  analysis_9: Counts distinct crash IDs where no damaged property was observed but damage level is above a certain threshold with valid insurance. 

  analysis_10: Identifies top five vehicle makes associated with speeding offenses by licensed drivers. 
    assumptions:
      - Assuming Joining Key with different source files are "CRASH_ID","UNIT_NBR". Comibining these two keys to make the record distinct.
