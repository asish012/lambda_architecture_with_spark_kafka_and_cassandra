package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}

object SparkUtils {

    val isIDE = {
        ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
    }

    def getSparkContext(appName : String) = {
        // checkpoint directory
        var checkpointDirectory = ""

        // get spark configuration
        val conf = new SparkConf().setAppName(appName)

        // Check if running from IDE
        if (isIDE) {
            conf.setMaster("local[*]")
            checkpointDirectory = "file:///Users/asishbiswas/VirtualBox VMs/Vagrant/spark-kafka-cassandra-applying-lambda-architecture/vagrant/checkpoint"
        } else {
            checkpointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
        }

        // setup spark context
        val sc = SparkContext.getOrCreate(conf)
        sc.setCheckpointDir(checkpointDirectory)
        sc
    }

    def getSqlContext(sc: SparkContext) = {
        // get sql context
        implicit val sqlContext = SQLContext.getOrCreate(sc)
        sqlContext
    }

    def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext,
                            sc : SparkContext,
                            d : Duration) = {
        val createStreamFunc = () => streamingApp(sc, d)
        val ssc = sc.getCheckpointDir match {
            case Some(checkpointDir) => {
                println(s"Checkpoint found $checkpointDir, Trying to retriving existing StreamingContext")
                StreamingContext.getActiveOrCreate(checkpointDir, createStreamFunc, sc.hadoopConfiguration, createOnError = true)
            }
            case None => {
                println("Checkpoint not found, Will create a new StreamingContext")
                StreamingContext.getActiveOrCreate(createStreamFunc)
            }
        }

        sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
        ssc
    }
}
