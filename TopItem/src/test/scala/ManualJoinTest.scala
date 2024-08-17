import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{Row, SparkSession}

class ManualJoinTest extends AnyFlatSpec with Matchers {

  "Manual Join Test" should "return joined common records" in {
    
	val spark = SparkSession.builder()
      .master("local[*]")
      .appName("ManualJoinTest")
      .getOrCreate()
    import spark.implicits._

    // Create sample data
    val saltedDetectionsRdd = spark.sparkContext.parallelize(Seq(
      (1, "detection1"),
      (2, "detection2"),
      (3, "detection3")
    ))

    val locationsRdd = spark.sparkContext.parallelize(Seq(
      (1, "location1"),
      (2, "location2"),
      (4, "location4")
    ))

    // Perform the manual join
    val joinedRdd = saltedDetectionsRdd.cartesian(locationsRdd)
      .filter { case (detection, location) => detection._1 == location._1 }
      .map { case (detection, location) => (detection._1, (detection._2, location._2)) }

    // Collect the results
    val results = joinedRdd.collect()

    // Assert the results
    results should contain allOf(
      (1, ("detection1", "location1")),
      (2, ("detection2", "location2"))
    )
  }
}