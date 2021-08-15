# Spark Assignment

### Read the following Datasets: 212, 4, 316
### Merge records based on identifier and Priority Matrix ( Can be file based or Spark config based )
### Intermediate result: One Row per identifier with attributes conforming to the priority matrix

## Tasks:
### Record Overlap between 4 and 316
### Distinct Identifier counts
### Datapartner wise count for Age records, Gender records, ZipCode records, 

## Final result: 
### Join the Intermediate result's Zipcode column with location Data's ZipCode column and add city field for corresponding records.
### Apply a UDF for bucketing the ages into groups :
    age <= 18 => age = 18
    18 < age <= 25 => age = 25
    25 < age <= 35 => age = 35
    35 < age <= 45 => age = 45
    45 < age <= 55 => age = 55
    55 < age <= 65 => age = 65
    65 < age => age = 75
### Show age and gender wise record count
### Write the joined output in any columnar format partitioned by age and gender column with single file per partition.
