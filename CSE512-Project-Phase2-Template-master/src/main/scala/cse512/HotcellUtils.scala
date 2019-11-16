package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // Utility that returns square of a number
  def square (a:Int) : Double =
  {
    (a * a).toDouble
  }

  // Method that counts the number of neighbors for each cell, given the min,max and input value.
  def CountNeighbours(minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, inputX: Int, inputY: Int, inputZ: Int): Int =
  {
    var numCells = 0
    // If the input is at the boundaries, increment the number of neighboring cells.
    if (inputX == minX || inputX == maxX) {
      numCells += 1
    }

    if (inputY == minY || inputY == maxY) {
      numCells += 1
    }

    if (inputZ == minZ || inputZ == maxZ) {
      numCells += 1
    }
    var returnValue = 26
    // Based on the number of closeby cells, return 17 (1 neighbor), 11 (2 neighbors), 7 (3 neighbors), 26 (none)
    numCells match {
      case 1 => returnValue = 17
      case 2 => returnValue = 11
      case 3 => returnValue = 7
      case _ => returnValue =  26
    }
    returnValue
  }

  def getGScore(x: Int, y: Int, z: Int, countN: Int, sumN: Int, numCells: Int, mean: Double, sd: Double): Double =
  {
    val numerator = sumN.toDouble - (mean * countN.toDouble)
    val denominator = sd * math.sqrt(((numCells.toDouble * countN.toDouble) - (countN.toDouble * countN.toDouble))/(numCells.toDouble - 1.0))
    numerator / denominator
  }
}