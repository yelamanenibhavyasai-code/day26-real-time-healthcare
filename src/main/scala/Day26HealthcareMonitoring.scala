
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object Day26HealthcareMonitoring {

  case class PatientVital(
    patientId: String,
    timestamp: String,
    heartRate: Int,
    spo2: Int,
    temperature: Double
  )

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day26HealthcareMonitoring")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("checkpoint/day26")
    ssc.sparkContext.setLogLevel("WARN")

    // Demo thresholds only; not for clinical decisions.
    val thresholds = Map(
      "heartRateMin" -> 60.0,
      "heartRateMax" -> 100.0,
      "spo2Min" -> 95.0,
      "temperatureMax" -> 38.0
    )

    val broadcastThresholds = ssc.sparkContext.broadcast(thresholds)

    // Demo metric. Accumulators are not an authoritative alert ledger.
    val abnormalReadingCount = ssc.sparkContext.longAccumulator(
      "Abnormal Vital Readings"
    )

    // Input: patientId,timestamp,heartRate,spo2,temperature
    // Example: P101,10:00:00,120,91,38.5
    val lines = ssc.socketTextStream("localhost", 9999)

    val vitals: DStream[PatientVital] = lines.flatMap { line =>
      val parts = line.trim.split(",")

      if (parts.length == 5) {
        try {
          val patientId = parts(0).trim
          val timestamp = parts(1).trim
          val heartRate = parts(2).trim.toInt
          val spo2 = parts(3).trim.toInt
          val temperature = parts(4).trim.toDouble

          if (patientId.nonEmpty && timestamp.nonEmpty &&
              heartRate >= 0 && spo2 >= 0 && temperature > 0) {
            Some(PatientVital(
              patientId, timestamp, heartRate, spo2, temperature
            ))
          } else {
            None
          }
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // Classify each reading using broadcast thresholds.
    val classified = vitals.map { vital =>
      val t = broadcastThresholds.value

      val abnormal =
        vital.heartRate < t("heartRateMin") ||
        vital.heartRate > t("heartRateMax") ||
        vital.spo2 < t("spo2Min") ||
        vital.temperature > t("temperatureMax")

      (vital.patientId, (vital, abnormal))
    }

    // Stateful: count consecutive abnormal batches per patient.
    val patientAbnormalState = classified
      .mapValues { case (_, abnormal) => if (abnormal) 1 else 0 }
      .reduceByKey(_ + _)
      .updateStateByKey[Int] {
        (newCounts: Seq[Int], previous: Option[Int]) =>
          val current = newCounts.sum
          if (current == 0) Some(0)
          else Some(previous.getOrElse(0) + current)
      }

    patientAbnormalState.foreachRDD { rdd =>
      rdd.filter(_._2 >= 2).collect().foreach {
        case (patientId, count) =>
          println(
            s"🚨 REPEATED ABNORMAL READINGS: patient=$patientId count=$count"
          )
      }
    }

    // Print abnormal readings and update demo accumulator.
    classified.foreachRDD { rdd =>
      val thresholdsInDriver = broadcastThresholds.value

      val abnormal = rdd.filter(_._2._2).collect()

      if (abnormal.nonEmpty) {
        println("\n===== ABNORMAL VITAL ALERTS =====")
        abnormal.foreach { case (patientId, (v, _)) =>
          println(
            s"ALERT patient=$patientId time=${v.timestamp} " +
            s"HR=${v.heartRate} SpO2=${v.spo2}% Temp=${v.temperature}C"
          )
        }
      }

      // Accumulator update in a single driver-side action.
      abnormalReadingCount.add(abnormal.length.toLong)

      println(
        s"Demo abnormal readings processed so far: ${abnormalReadingCount.value}"
      )
    }

    // Window: number of readings per patient in the latest 60 seconds.
    val recentCounts = vitals
      .map(v => (v.patientId, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(60),
        Seconds(10)
      )

    recentCounts.foreachRDD { rdd =>
      println("\n===== PATIENT READINGS: LAST 60 SECONDS =====")
      rdd.collect().sortBy(_._1).foreach {
        case (patientId, count) =>
          println(s"$patientId -> $count readings")
      }
    }

    println("Day 26: Real-Time Healthcare Monitoring")
    println("Batch interval: 10 seconds")
    println("Window size: 60 seconds")
    println("Input socket: localhost:9999")

    ssc.start()
    ssc.awaitTermination()
  }
}
