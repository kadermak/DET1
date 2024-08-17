import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object TopItemsRank {

  def main(args: Array[String]): Unit = {

	// Config for inputfile and TopX
    val inputDetectionsParquetFilePath = "detections.parquet"
    val inputLocationsParquetFilePath = "locations.parquet"
    val outputFilePath = "output.txt"
    val topX = 5
    // Config for inputfile and TopX
    
	try {
	// start spark session
	val spark = SparkSession.builder()
      .appName("TopItemsPerLocationWithSalting")
      .master("local[*]")
      .getOrCreate()

    
    import spark.implicits._
    
	val sc: SparkContext = spark.sparkContext

    // Define schema for the detections and locations
    val detectionsSchema = StructType(Array(
      StructField("detection_oid", IntegerType, nullable = true),
      StructField("geographical_location_oid", IntegerType, nullable = true),
      StructField("item_name", StringType, nullable = true)
    ))

    val locationsSchema = StructType(Array(
      StructField("geographical_location_oid", IntegerType, nullable = true),
      StructField("geographical_location", StringType, nullable = true)
    ))

    // Load the parquet files
    val detectionsRdd: RDD[Row] = spark.read.schema(detectionsSchema).parquet(inputDetectionsParquetFilePath).rdd
    val locationsRdd: RDD[Row] = spark.read.schema(locationsSchema).parquet(inputLocationsParquetFilePath).rdd

    // Drop duplicates based on detection_oid
    val distinctDetectionsRdd = detectionsRdd.map(r => (r.getInt(0), r)).reduceByKey((x, y) => x).map(_._2)

    // Add salt to avoid data skew
    val saltedDetectionsRdd = distinctDetectionsRdd.map(r => (
      r.getInt(0), // detection_oid
      r.getInt(1), // geographical_location_oid
      r.getString(2), // item_name
      scala.util.Random.nextInt(10) // salt
    ))
	
	/*
    // default .join commentout , use manual join Join the detections and locations RDDs on geographical_location_oid
    //  val joinedRdd = saltedDetectionsRdd.map(r => (r._2, r)).join(locationsRdd.map(r => (r.getInt(0), r)))
	*/
	println("PROCESSING TOP ITEM###")
	
	// Manual join e detections and locations RDDs on geographical_location_oid
	val joinedRdd = saltedDetectionsRdd.cartesian(locationsRdd)
	.filter { case (detection, location) => detection._2 == location.getInt(0) }
	.map { case (detection, location) => (detection._2, (detection, location)) }
	 
   
    // Group by location, count items (distinct by detection_oid), sort by count descending
    val groupedRdd = joinedRdd.map { case (_, (detection, location)) =>
      ((detection._2, detection._3, location.getString(1)), 1)
    }.reduceByKey(_ + _)

    // Sum across salts for data scew
    val summedRdd = groupedRdd.map { case ((geoLocOid, itemName, locationName), count) =>
      ((geoLocOid, itemName), count)
    }.reduceByKey(_ + _)
 
    val topItemsPerLocationRdd = groupedRdd.map { case ((geoLocOid, itemName, locationName), count) =>
    (geoLocOid, itemName, count)
    }.sortBy(_._3, ascending = false)

    // Assign item ranks (across all locations)
	val rankedRdd = topItemsPerLocationRdd.zipWithIndex().map { case ((geoLocOid, itemName, count), itemRank) =>
    (geoLocOid, itemName, count, itemRank + 1)}

    // Filter output to return only the top X items  
	val outputRdd = rankedRdd.filter(_._4 <= topX)
	
	// Print Output
	outputRdd.collect().foreach { case (geoLocOid, itemName, count, itemRank ) =>
	println(s"LocationID: $geoLocOid,  Item_Rank: $itemRank , Item_Name: $itemName, Item_Count: $count,")
	}
    
	/*
    // Write output to text file
    topItemsPerLocationRdd.map { case (geoLocOid, itemName, count, itemRank, localRank) =>
      s"$geoLocOid: $itemName ($count) Global Rank: $itemRank Local Rank: $localRank"
    }.saveAsTextFile(outputFilePath)
    */
    println("COMPLETED###")
    spark.stop()
	
	} catch {
    case e: Exception => println(s"Error: ${e.getMessage}")
	}
  
  } // end main
}
