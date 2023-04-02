import scala.io.Source
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.getquill.*

import scala.util.Try
import scala.math.Ordering.*

def getDbContext(dbName: String, port: Int): PostgresJdbcContext[LowerCase.type] =
  val pgDataSource = new org.postgresql.ds.PGSimpleDataSource()
  pgDataSource.setUser("scalauser")
  pgDataSource.setPassword("pwd123")
  pgDataSource.setDatabaseName(dbName)
  pgDataSource.setPortNumbers(Array(port))
  val config = new HikariConfig()
  config.setDataSource(pgDataSource)
  new PostgresJdbcContext(LowerCase, new HikariDataSource(config))

case class BirthsDeaths(
  year: Int,
  ethnicity: String,
  deathRate: Double,
  birthRate: Double
)

case class PopulationDistribution(
  year: Int,
  ethnicity: String,
  gender: String,
  ageGroupLow: Int,
  ageGroupHigh: Int,
  population: Int
)

/**
 * EXERCISE 1: The total population of Singapore for the most recently available year (2022).
 * @return An option containing the total population for 2022, if it can be computed from the available data.
 */
@main
def totalPopulation(): Option[Int] =
  val dbContext = getDbContext("population", 5432)
  import dbContext._
  run {
    quote {
      query[PopulationDistribution]
        .filter(row => row.ageGroupLow <= 65 && row.year ==2022)
        .map(_.population)
        .sum
    }
  }

/**
 * EXERCISE 2: The total population of Singapore for the most recently available year (2022), by gender.
 * @return A list of pairs, whereby each gender is coupled with its population for 2022.
 *         .
 */
@main
def totalPopulationByGender(): List[(String, Int)] =
  val dbContext = getDbContext("population", 5432)
  import dbContext._
  run {
    quote {
      query[PopulationDistribution]
        .filter(row => row.year == 2022 && row.ageGroupLow <= 65)
        .groupByMap(_.gender)(p => (p.gender, sum(p.population)))
    }
  }

/**
 * EXERCISE 3: The number of children born in 2021, by ethnicity.
 * @return A list of pairs, where each ethnicity is paired with its number of births for that year.
 */
@main
def childrenBornIn2021ByEthnicity(): List[(String, Double)] =
  val dbContext = getDbContext("population", 5432)

  import dbContext._
  run {
    quote {
      query[PopulationDistribution]
        .filter(row => row.year == 2021 && row.ageGroupLow <= 65)
        .groupByMap(_.ethnicity)(row => (row.ethnicity, sum(row.population)))
        .join(query[BirthsDeaths]).on((l, r) => l._1 == r.ethnicity && r.year == 2021)
        .map {
          case ((e, pop), bd) => (e, pop * bd.birthRate)
        }
    }
  }

case class Temp(year: Int, population: Double)
/**
 * EXERCISE 4: The number of children born every year.
 * @return A list of pairs, where each year is paired with its number of births for that year.
 */
@main
def childrenBornEveryYear(): List[(Int, Double)] =
  val dbContext = getDbContext("population", 5432)

  import dbContext._
  run {
    quote {
      query[PopulationDistribution]
        .filter(_.ageGroupLow <= 65)
        .join(query[BirthsDeaths]).on((l, r) => l.year == r.year && l.ethnicity == r.ethnicity)
        .map { case (p, b) => (p.year, p.population * b.birthRate / 1000)}
        .nested
        .groupByMap(_._1)(row => (row._1, sum(row._2)))
        .sortBy(_._1)
    }
  }

/**
 * EXERCISE 5: Yearly variance of the population's growth rate, by ethnicity.
 * @return A list of triples, where each year and ethnicity is coupled with its number of births for that year.
 */
@main
def growthRateYearlyVariance(): List[(Int, String, Double)] =
  val dbContext = getDbContext("population", 5432)

  import dbContext._
  ???