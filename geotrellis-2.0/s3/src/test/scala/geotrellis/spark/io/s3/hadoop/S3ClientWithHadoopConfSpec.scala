package geotrellis.spark.io.s3.hadoop

import geotrellis.spark.io.s3.S3Client
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

class S3ClientWithHadoopConfSpec extends FunSuite {

  test("Test access huawei obs") {
    import org.apache.hadoop.fs.s3a.Constants._
    import geotrellis.spark.io.s3.S3Client._

    val hadoopConf = new Configuration()
    hadoopConf.set(ACCESS_KEY, "9E4YAAKGMSERKEEUDXCH")
    hadoopConf.set(SECRET_KEY, "yq6jTN2HfDdOnvMRaaINF81SiCmpfReeF3a9A6PV")
    hadoopConf.set(REGION, "cn-east-2")
    hadoopConf.set(ENDPOINT, "obs.cn-east-2.myhwclouds.com")

    val s3Client = S3Client.hadoopDefaultAwsClient(hadoopConf)
    val seq = s3Client.listKeys("obs-81d5", "image")
    seq.foreach(println)
  }

}
