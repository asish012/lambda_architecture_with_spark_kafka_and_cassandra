package streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.SparkUtils._

object StreamingJob {
    def main(args: Array[String]): Unit = {
        // setup spark context
        val sc = getSparkContext("Lambda with Spark")

        val batchDuration = Seconds(4)
        val ssc = new StreamingContext(sc, batchDuration)

        val inputPath = isIDE match {
            case true => "file:///Users/asishbiswas/VirtualBox VMs/Vagrant/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"    // running in IDE
            case false => "file:///vagrant/input"   // running in VM
        }

        val textDStream = ssc.textFileStream(inputPath)
        textDStream.print()

        ssc.start()             // returns immediately, we need to wait while streaming data starts and our operation can continue
        ssc.awaitTermination()  // wait indefinitely (we'll close the app manually)
    }
}
