import scala.io.Source
import scala.util.Try
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.getquill.*

@main
def setupDB(): Unit = // Run this first, may take a couple of minutes to complete
  uploadBirthsDeaths()
  uploadPopulationData()

@main
def cleanBirthDeathData(): Iterator[BirthsDeaths] =
  val reader = Source
    .fromResource("crude-birth-death-natural-increase-rates-by-ethnic-group-from-1971-onwards.csv")
    .getLines()
    .drop(1)

  val Fields = "(\\d\\d\\d\\d),(Chinese|Malays|Indians|Others),(\\d+(\\.(\\d)*)*),(\\d+(\\.(\\d)*)*),(\\d+(\\.(\\d)*)*)".r
  reader.map { case Fields(year, ethnicity, death, _, _, birth, _, _, growth, _, _) =>
    BirthsDeaths(year.toInt, ethnicity, death.toDouble, birth.toDouble)
  }

@main
def uploadBirthsDeaths(): Unit =
  val dbContext = getDbContext("population", 5432)
  import dbContext._

  val createstmt = dataSource.getConnection.prepareStatement(
    " create table if not exists birthsdeaths(year int, ethnicity varchar(50), deathrate real, birthrate real)"
  )
  createstmt.execute()
  val emptytblstmt = dataSource.getConnection.prepareStatement(
    "delete from birthsdeaths"
  )
  emptytblstmt.execute()
  cleanBirthDeathData().foreach { case BirthsDeaths(year, ethnicity, deathRate, birthRate) =>
    run {
      quote {
        query[BirthsDeaths]
          .insertValue(
            BirthsDeaths(
              lift(year),
              lift(ethnicity),
              lift(deathRate),
              lift(birthRate)
            )
          )
      }
    }
  }

def cleanPopulationData(): Iterable[PopulationDistribution] =
  val reader = Source
    .fromResource("population-raw.csv")
    .getLines()

  val tokens: Array[Array[String]] = reader.map(_.split(',').map(_.trim)).to(Array)

  tokens(0).tail.zipWithIndex.flatMap { case (year, i) =>
    tokens.tail.zipWithIndex.map{ case (row, j) =>
      val (low, high) = parseAgeGroup(row(0))
      val ethnicity = parseEthnicity(row(0))
      val gender = parseGender(row(0))
      val pop = Try(row(i + 1).toInt).toOption.getOrElse(0)
      PopulationDistribution(year.toInt, ethnicity, gender, low, high, pop)
    }
  }
def parseAgeGroup(s: String): (Int, Int) =
  val parserLH = "\\d+ - \\d+ Years".r
  val parserL = "\\d+ Years & Over".r
  val parserAge = "\\d+".r
  parserLH
    .findFirstIn(s)
    .map(f => {val p = parserAge.findAllIn(f) ; (p.next.toInt, p.next.toInt)})
    .orElse(parserL.findFirstIn(s).map(f => (parserAge.findFirstIn(f).get.toInt, 95)))
    .get

def parseEthnicity(s: String): String =
  val parser = "Chinese|Malays|Indians|Others".r
  parser.findFirstIn(s).get

def parseGender(s: String): String =
  val parser = "Male|Female".r
  parser.findFirstIn(s).getOrElse("Male & Female")

@main
def uploadPopulationData(): Unit =
  val dbContext = getDbContext("population", 5432)

  import dbContext._

  val createstmt = dataSource.getConnection.prepareStatement(
    " create table if not exists populationdistribution(year int, ethnicity varchar(50), gender varchar(50), agegrouplow int, agegrouphigh int, population int)"
  )

  createstmt.execute()
  val emptytblstmt = dataSource.getConnection.prepareStatement(
    "delete from populationdistribution"
  )
  emptytblstmt.execute()

  cleanPopulationData().foreach { case PopulationDistribution(year, ethnicity, gender, low, high, pop) =>
      run {
        quote {
          query[PopulationDistribution].insertValue(PopulationDistribution(
            lift(year.toInt),
            lift(ethnicity),
            lift(gender),
            lift(low),
            lift(high),
            lift(pop)
          ))
        }
      }
    }
