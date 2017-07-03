package com.image

import com.image.util.{ImageFileOutputFormat, ImageInputDStream}
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

object ImageStreaming {

  def imageModel(image : BytesWritable) = image.getLength

  def main(args: Array[String]) {
		if (args.length < 3) {
      		System.err.println("Usage: ImageStreaming <seconds> <hostname> <port>")
      		System.exit(1)
    }
   	val s = new SparkConf().setMaster("local[2]").setAppName("face")
											  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 		val sc = new SparkContext(s)
 		val ssc = new StreamingContext(sc, Seconds(args(0).toInt))
 		val img = new ImageInputDStream(ssc, args(1), args(2).toInt,
        		           StorageLevel.MEMORY_AND_DISK_SER)//调用重写的 ImageInputDStream 方法读取图片
 		val imgMap = img.map(x => (new Text(System.currentTimeMillis().toString), x))
 		imgMap.saveAsNewAPIHadoopFiles("hdfs://localhost:9000/user/hadoop/image/", "", classOf[Text],
        		   classOf[BytesWritable], classOf[ImageFileOutputFormat],
                		       ssc.sparkContext.hadoopConfiguration)//调用 ImageFileOutputFormat 方法写入图片
 
 		imgMap.map(x => (x._1, {
 			if (x._2.getLength > 0) imageModel(x._2) else "-1"
 		}))//获取 key 的值，即图片
 		.filter(x => x._2 != "0" && x._2 != "-1")
 		.map(x => "{time:" + x._1.toString +","+ x._2 + "},").print()
 
 		ssc.start()
		ssc.awaitTermination()
	}
}