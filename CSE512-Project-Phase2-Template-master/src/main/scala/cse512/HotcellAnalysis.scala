package cse512

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //pickupInfo.show()
    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 0))
    spark.udf.register("CalculateY", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 1))
    spark.udf.register("CalculateZ", (pickupTime: String) => HotcellUtils.CalculateCoordinate(pickupTime, 2))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.createOrReplaceTempView("pickupinfo")
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    // Register the helper for square
    spark.udf.register("square", (inputX: Int) => HotcellUtils.square(inputX))

    // Register the count Neighbors function.
    spark.udf.register("CountNeighbors", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, inputX: Int, inputY: Int, inputZ: Int)
    => HotcellUtils.CountNeighbours(minX, minY, minZ, maxX, maxY, maxZ, inputX, inputY, inputZ))
    //Register the Gscore method
    spark.udf.register("GScore", (x: Int, y: Int, z: Int, countn: Int, sumn: Int, numcells: Int, mean: Double, sd: Double) => HotcellUtils.getGScore(x, y, z, countn, sumn, numcells, mean, sd))

    // First get the points that fall into the cube.
    val givenPoints = spark.sql("select x, y, z from pickupinfo where x >= " + minX + " and y >= " + minY  + " and z >= " + minZ + " and x <= " + maxX + " and y <= " + maxY +  " and z <= " + maxZ + " order by z, y, x").persist()
    givenPoints.createOrReplaceTempView("givenPoints")
    givenPoints.show()

    // Get the points and the number of values for each set
    val pointsAndCount = spark.sql("select x, y, z, count(*) as pointValues from givenPoints group by z, y, x order by z, y, x").persist()
    pointsAndCount.createOrReplaceTempView("pointsAndCount")
    pointsAndCount.show()

    // Calculate the sum and the sum of the Squares of the points.
    val sumofPoints = spark.sql("select sum(pointValues) as sumVal, sum(square(pointValues)) as squaredSum from pointsAndCount")
   // val sumofPoints = spark.sql("select count(*) as countval, sum(pointValues) as sumVal,sum(square(pointValues)) as squaredSum from pointsAndCount")
    sumofPoints.createOrReplaceTempView("sumofPoints")
    sumofPoints.show()

    // Use these to find the mean and Standard Deviation
    val sumVal = sumofPoints.first().getLong(0)
    val squaredSum = sumofPoints.first().getDouble(1)

    val mean = sumVal.toDouble / numCells.toDouble
    val SD = math.sqrt((squaredSum.toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))

    // Calculate the neighbors count
    val Neighbours = spark.sql("select CountNeighbors(" + minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "a1.x, a1.y, a1.z) as nCount, count(*) as countAll, a1.x as x, a1.y as y, a1.z as z, sum(a2.pointValues) as sumTotal from pointsAndCount as a1, pointsAndCount as a2 where (a2.x = a1.x + 1 or a2.x = a1.x or a2.x = a1.x - 1) and (a2.y = a1.y + 1 or a2.y = a1.y or a2.y =a1.y - 1) and (a2.z = a1.z + 1 or a2.z = a1.z or a2.z = a1.z - 1) group by a1.z, a1.y, a1.x order by a1.z, a1.y, a1.x").persist()
    Neighbours.createOrReplaceTempView("NeighborsCount")
    Neighbours.show()

    // Calcuate the Gscores
    val GScoreDF = spark.sql("select GScore(x, y, z, ncount, sumtotal, "+ numCells + ", " + mean + ", " + SD + ") as gscore, x, y, z from NeighborsCount order by gscore desc");
    GScoreDF.createOrReplaceTempView("GScoreDF")
    GScoreDF.show()

    // Prepare the final view and return
    val finalresult = spark.sql("select x, y, z from GScoreDF")
    finalresult.createOrReplaceTempView("finalresult")
    finalresult.show()
    finalresult
  }
}