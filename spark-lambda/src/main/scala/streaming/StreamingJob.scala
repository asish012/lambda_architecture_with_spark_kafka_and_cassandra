package streaming

import domain.{Activity, ActivityByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils.SparkUtils._

object StreamingJob {
    def main(args: Array[String]): Unit = {
        val sc = getSparkContext("Lambda with Spark")
        implicit val sqlContext = new SQLContext(sc)
        val batchDuration = Seconds(4)

        // import implicit conversion functions
        import org.apache.spark.sql.functions._
        import sqlContext.implicits._

        // setup streaming context in case no checkpoint directory was defined or checkpoint is invalid
        val streamingApp = (sc : SparkContext, d : Duration) => {
            println("Checkpoint invalid or not defined, Creating new StreamingContext")
            val ssc = new StreamingContext(sc, batchDuration)
            ssc
        }

        // setup streaming context from checkpoint or a new one using the above fn
        val ssc = getStreamingContext(streamingApp, sc, batchDuration)

        val inputPath = isIDE match {
            case true => "file:///Users/asishbiswas/VirtualBox VMs/Vagrant/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"    // running in IDE
            case false => "file:///vagrant/input"   // running in VM
        }

        val textDStream = ssc.textFileStream(inputPath)

        // prepare stream of activityRDD
        val activityDStream = textDStream.transform{ inputRDD =>
            inputRDD.flatMap{ line =>
                val record = line.split("\\t")
                val MS_IN_HOUR = 60 * 60 * 1000
                if (record.length == 7)
                    Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
                else
                    None
            }
        }

        val activityByProductDStream = activityDStream.transform{ activity =>
            val activityDF = activity.toDF()
            activityDF.registerTempTable("activity")

            val activityByProductDF = sqlContext.sql(
                """SELECT
                  |timestamp_hour,
                  |product,
                  |sum(case when action = 'purchase'    then 1 else 0 end) as purchase_count,
                  |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                  |sum(case when action = 'page_view'   then 1 else 0 end) as page_view_count
                  |FROM activity
                  |GROUP BY timestamp_hour, product
                """.stripMargin)

            // [K : (String, Long), V : ActivityByProduct]
            activityByProductDF.map{ r =>
                ( (r.getLong(0), r.getString(1)),
                ActivityByProduct(r.getLong(0), r.getString(1), r.getLong(2), r.getLong(3), r.getLong(4)) )}

        }
        activityByProductDStream.print()

        ssc.start()             // returns immediately, we need to wait while streaming data starts and our operation can continue
        ssc.awaitTermination()  // wait indefinitely (we'll close the app manually)
    }
}
