package streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._

import domain.{Activity, ActivityByProduct}
import utils.SparkUtils._
import functions._

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

//        // DStream to RDD, RDD to DF, Find activity by product
//            val statefulActivityByProductDStream = activityDStream.transform{ rawActivityRDD =>
//                val activityDF = activity.toDF()
//                activityDF.registerTempTable("activity")
//
//                val activityByProductDF = sqlContext.sql(
//                    """SELECT
//                      |timestamp_hour,
//                      |product,
//                      |sum(case when action = 'purchase'    then 1 else 0 end) as purchase_count,
//                      |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
//                      |sum(case when action = 'page_view'   then 1 else 0 end) as page_view_count
//                      |FROM activity
//                      |GROUP BY timestamp_hour, product
//                    """.stripMargin)
//
//                // return a key-valued dataframe. [Key : (String, Long), Value : ActivityByProduct]
//                activityByProductDF.map( r => {
//                    ( (r.getLong(0), r.getString(1)),
//                    ActivityByProduct(r.getLong(0), r.getString(1), r.getLong(2), r.getLong(3), r.getLong(4)) )})

        // Same operation from RDD level, but without going too deep to DataFrame.
        // DStream to RDD to updateState

        val spec = StateSpec.function(mapActivityStateFunction).timeout(Seconds(30))

        val statefulActivityByProductDStream = activityDStream.transform{ rawActivityRDD =>
            val productActivityMapRDD = rawActivityRDD.keyBy(a => (a.timestamp_hour, a.product)).cache()

            productActivityMapRDD.mapValues(a =>
                a.action match {
                    case "purchase"     => ActivityByProduct(a.timestamp_hour, a.product, 1, 0, 0)
                    case "add_to_cart"  => ActivityByProduct(a.timestamp_hour, a.product, 0, 1, 0)
                    case "page_view"    => ActivityByProduct(a.timestamp_hour, a.product, 0, 0, 1)
                }
            )
        }.mapWithState(spec)

        statefulActivityByProductDStream.print()


    // UpdateStateByKey (old way of updating state) //
    /* .updateStateByKey((newItemsPerKey : Seq[ActivityByProduct], currentState : Option[(Long, Long, Long, Long)]) => {
            var (prevTimestamp, purchase_count, add_to_cart_count, page_view_count) = currentState.getOrElse((0L, 0L, 0L, 0L))
            var newState : Option[(Long, Long, Long, Long)] = null

            if (newItemsPerKey.isEmpty) {
                // if no new previous state is found, and the old state is very old, remove the state
                if (System.currentTimeMillis() - prevTimestamp > 30000 + 4000) { // 30s = timeout | 4s = batch interval
                    // haven't found items that corresponds current key, but found an old state. lets remove the state.
                    newState = None
                } else {
                    // haven't found items that corresponds current key, neither found any previous state. lets give the key a state with current-timestamp.
                    newState = Some((prevTimestamp, purchase_count, add_to_cart_count, page_view_count))
                }
            } else {
                // previous state is present, aggregate current with previous state
                newItemsPerKey.foreach(a => {
                    purchase_count += a.purchase_count
                    add_to_cart_count += a.add_to_cart_count
                    page_view_count += a.page_view_count
                })

                newState = Some((System.currentTimeMillis(), purchase_count, add_to_cart_count, page_view_count))
            }
            newState
        }).print()
    */

//        for Zeppelin to display statefulActivityByProduct from registered table
//        statefulActivityByProductDStream.foreachRDD(rdd => {
//            rdd.toDF().registerTempTable("statefulActiityByProduct")
//        })


        ssc.start()             // returns immediately, we need to wait while streaming data starts and our operation can continue
        ssc.awaitTermination()  // wait indefinitely (we'll close the app manually)
    }
}
