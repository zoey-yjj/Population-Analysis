## The Input Data

The source of the data is data.gov.sg. From there we have downloaded 2 files into the resources folder.

 * `population-raw.csv` containing the evolution of the Singapore population by year, gender, and ethnicity

 * `crude-birth-death-natural-increase-rates-by-ethnic-group-from-1971-onwards.csv` containing the birth and death rates (per thousand)

## The Database

Set up in Docker container for Postgresql database server, listening on port 5432 on the local machine.

Set up a database called `population`, accessible by the user `scalauser` with the password `pwd123`.

Functions in `CleanDataAndLoadDB.scala` helps cleans the input data and then uploads it into database tables. 

## Run the loading code

Once the database `population` is set up, run the `setupDB` method to create 2 tables:

   * `case class BirthsDeaths(year: Int, ethnicity: String, deathRate: Double, birthRate: Double)`
   * `case class PopulationDistribution(year: Int, ethnicity: String, gender: String, ageGroupLow: Int, ageGroupHigh: Int, population: Int)`

## The Tasks

Write queries in Quill to pass QuillTest.

1. The total population of Singapore.

2. The total population of Singapore by gender.

3. The number of children born by ethnicity.

4. The number of children born every year.

5. Yearly variance of the population's growth rate, by ethnicity.
