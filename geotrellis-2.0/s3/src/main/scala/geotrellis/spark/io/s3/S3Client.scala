/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.io.s3

import geotrellis.util.LazyLogging
import com.amazonaws.auth._
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.s3.model._
import java.io.{ByteArrayInputStream, InputStream}

import com.amazonaws.Protocol
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.{AnonymousAWSCredentialsProvider, BasicAWSCredentialsProvider}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

trait S3Client extends LazyLogging with Serializable {

  def doesBucketExist(bucket: String): Boolean

  def doesObjectExist(bucket: String, key: String): Boolean

  def listObjects(listObjectsRequest: ListObjectsRequest): ObjectListing

  def listObjects(bucketName: String, prefix: String): ObjectListing =
    listObjects(new ListObjectsRequest(bucketName, prefix, null, null, null))

  def listKeys(bucketName: String, prefix: String): Seq[String] =
    listKeys(new ListObjectsRequest(bucketName, prefix, null, null, null))

  def listKeys(listObjectsRequest: ListObjectsRequest): Seq[String]

  def getObject(getObjectRequest: GetObjectRequest): S3Object

  def putObject(putObjectRequest: PutObjectRequest): PutObjectResult

  def listNextBatchOfObjects(listing: ObjectListing): ObjectListing

  @tailrec
  final def deleteListing(bucket: String, listing: ObjectListing): Unit = {
    val listings = listing
      .getObjectSummaries
      .asScala
      .map { os => new KeyVersion(os.getKey) }
      .toList

    // Empty listings cause malformed XML to be sent to AWS and lead to unhelpful exceptions
    if (listings.nonEmpty) {
      deleteObjects(bucket, listings)
      if (listing.isTruncated) deleteListing(bucket, listNextBatchOfObjects(listing))
    }
  }

  def deleteObject(deleteObjectRequest: DeleteObjectRequest): Unit

  def copyObject(copyObjectRequest: CopyObjectRequest): CopyObjectResult

  def deleteObjects(deleteObjectsRequest: DeleteObjectsRequest): Unit

  def getObject(bucketName: String, key: String): S3Object =
    getObject(new GetObjectRequest(bucketName, key))

  def deleteObjects(bucketName: String, keys: List[KeyVersion]): Unit = {
    val objectsDeleteRequest = new DeleteObjectsRequest(bucketName)
    objectsDeleteRequest.setKeys(keys.asJava)
    deleteObjects(objectsDeleteRequest)
  }

  def copyObject(sourceBucketName: String, sourceKey: String,
    destinationBucketName: String, destinationKey: String): CopyObjectResult =
    copyObject(new CopyObjectRequest(sourceBucketName, sourceKey, destinationBucketName, destinationKey))

  def deleteObject(bucketName: String, key: String): Unit =
    deleteObject(new DeleteObjectRequest(bucketName, key))

  def putObject(bucketName: String, key: String, input: InputStream, metadata: ObjectMetadata): PutObjectResult =
    putObject(new PutObjectRequest(bucketName, key, input, metadata))

  def putObject(bucketName: String, key: String, bytes: Array[Byte], metadata: ObjectMetadata): PutObjectResult = {
    metadata.setContentLength(bytes.length)
    putObject(bucketName, key, new ByteArrayInputStream(bytes), metadata)
  }

  def putObject(bucketName: String, key: String, bytes: Array[Byte]): PutObjectResult =
    putObject(bucketName, key, bytes, new ObjectMetadata())

  def readBytes(bucketName: String, key: String): Array[Byte] =
    readBytes(new GetObjectRequest(bucketName, key))

  def readBytes(getObjectRequest: GetObjectRequest): Array[Byte]

  def readRange(start: Long, end: Long, getObjectRequest: GetObjectRequest): Array[Byte]

  def getObjectMetadata(bucketName: String, key: String): ObjectMetadata =
    getObjectMetadata(new GetObjectMetadataRequest(bucketName, key))

  def getObjectMetadata(getObjectMetadataRequest: GetObjectMetadataRequest): ObjectMetadata

  def listObjectsIterator(bucketName: String, prefix: String, maxKeys: Int = 0): Iterator[S3ObjectSummary] =
    listObjectsIterator(new ListObjectsRequest(bucketName, prefix, null, null, if (maxKeys == 0) null else maxKeys))

  def listObjectsIterator(request: ListObjectsRequest): Iterator[S3ObjectSummary] =
    new Iterator[S3ObjectSummary] {
      var listing = listObjects(request)
      var iter = listing.getObjectSummaries.asScala.iterator

      def getNextPage: Boolean =  {
        listing.isTruncated && {
          val nextRequest = request.withMarker(listing.getNextMarker)
          listing = listObjects(nextRequest)
          iter = listing.getObjectSummaries.asScala.iterator
          iter.hasNext
        }
      }

      def hasNext: Boolean = {
        iter.hasNext || getNextPage
      }

      def next: S3ObjectSummary = iter.next
    }

  def setRegion(region: com.amazonaws.regions.Region): Unit
}

object S3Client {
  val DEFAULT_HUAWEI_OBS_ENDPOINT = "obs.cn-north-1.myhwclouds.com"
  val DEFAULT_HUAWEI_OBS_REGION = "cn-north-1"
  val REGION = "fs.s3a.region"

  def defaultConfiguration = {
    val config = new com.amazonaws.ClientConfiguration
    config.setMaxConnections(128)
    config.setMaxErrorRetry(16)
    config.setConnectionTimeout(100000)
    config.setSocketTimeout(100000)
    config.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(32))

    config
  }

  def DEFAULT = {
//    val s3Client = new com.amazonaws.services.s3.AmazonS3Client(new DefaultAWSCredentialsProviderChain(), defaultConfiguration)
//    AmazonS3Client(s3Client)

    val accessKey = "IVQZFVJUKRBCI2FQKICH"
    val secretKey = "eTa1m1oPCMurx4eKaVnJtuJVZUxpPruMqr2ecfmp"
    val endPoint = "obs.cn-north-1.myhwclouds.com"

    val credentials = new AWSCredentialsProviderChain(new BasicAWSCredentialsProvider(accessKey, secretKey),
      new AnonymousAWSCredentialsProvider())
    val s3Client = new com.amazonaws.services.s3.AmazonS3Client(credentials, defaultConfiguration)
    s3Client.setEndpoint(endPoint)
    AmazonS3Client(s3Client)
  }

  def ANONYMOUS = {
    val s3Client = new com.amazonaws.services.s3.AmazonS3Client(new AnonymousAWSCredentials(), defaultConfiguration)
    AmazonS3Client(s3Client)
  }

  def awsClientFromConf(conf: Map[String, String]) = {
    import org.apache.hadoop.fs.s3a.Constants._
    val accessKey = conf.get(ACCESS_KEY)
    val secretKey = conf.get(SECRET_KEY)
    val endPoint = conf.getOrElse(ENDPOINT, DEFAULT_HUAWEI_OBS_ENDPOINT)
    if (accessKey.isDefined && secretKey.isDefined) {
      val credentials = new AWSCredentialsProviderChain(new BasicAWSCredentialsProvider(accessKey.get, secretKey.get),
        new AnonymousAWSCredentialsProvider())
      val s3Client = new com.amazonaws.services.s3.AmazonS3Client(credentials, defaultConfiguration)
      s3Client.setEndpoint(endPoint)
      AmazonS3Client(s3Client)
    } else {
      DEFAULT
    }
  }

  def hadoopConf2Map(hadoopConf: Configuration): Map[String, String] = {
    Map("fs.s3a.access.key" -> hadoopConf.get("fs.s3a.access.key", ""),
      "fs.s3a.secret.key" -> hadoopConf.get("fs.s3a.secret.key", ""),
      "fs.s3a.region" -> hadoopConf.get("fs.s3a.region", ""),
      "fs.s3a.endpoint" -> hadoopConf.get("fs.s3a.endpoint", ""))
  }

  def hadoopDefaultAwsClient(hadoopConf: Configuration) = {
    import org.apache.hadoop.fs.s3a.Constants._

    val accessKey = hadoopConf.get(ACCESS_KEY, null)
    val secretKey = hadoopConf.get(SECRET_KEY, null)
    val endpoint = hadoopConf.get(ENDPOINT, DEFAULT_HUAWEI_OBS_ENDPOINT)
    val region = hadoopConf.get(REGION, DEFAULT_HUAWEI_OBS_REGION)

    val credentials = new AWSCredentialsProviderChain(new BasicAWSCredentialsProvider(accessKey, secretKey),
      new AnonymousAWSCredentialsProvider())

    val awsConf = getHadoopDefaultAwsConf(hadoopConf)
    val s3Client = new com.amazonaws.services.s3.AmazonS3Client(credentials, awsConf)
    s3Client.setEndpoint(endpoint)

    //TODO shoud use this method after spark serverless upgrade
//    val s3Client = AmazonS3ClientBuilder.standard()
//      .withCredentials(credentials)
//      .withClientConfiguration(awsConf)
//      .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
    AmazonS3Client(s3Client)
  }

  def getHadoopDefaultAwsConf(hadoopConf: Configuration): com.amazonaws.ClientConfiguration = {
    val awsConf = defaultConfiguration

    import org.apache.hadoop.fs.s3a.Constants._
    awsConf.setMaxConnections(hadoopConf.getInt(MAXIMUM_CONNECTIONS, 128))
    val secureConnections = hadoopConf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS)
    awsConf.setProtocol(if (secureConnections) Protocol.HTTPS
    else Protocol.HTTP)
    awsConf.setMaxErrorRetry(hadoopConf.getInt(MAX_ERROR_RETRIES, 10))
    awsConf.setConnectionTimeout(hadoopConf.getInt(ESTABLISH_TIMEOUT, 100000))
    awsConf.setSocketTimeout(hadoopConf.getInt(SOCKET_TIMEOUT, 100000))

    val proxyHost = hadoopConf.getTrimmed(PROXY_HOST, "")
    val proxyPort = hadoopConf.getInt(PROXY_PORT, -1)
    if (!proxyHost.isEmpty) {
      awsConf.setProxyHost(proxyHost)
      if (proxyPort >= 0) awsConf.setProxyPort(proxyPort)
      else if (secureConnections) {
        awsConf.setProxyPort(443)
      }
      else {
        awsConf.setProxyPort(80)
      }
      val proxyUsername = hadoopConf.getTrimmed(PROXY_USERNAME)
      val proxyPassword = hadoopConf.getTrimmed(PROXY_PASSWORD)
      if ((proxyUsername == null) != (proxyPassword == null)) {
        val msg = "Proxy error: " + PROXY_USERNAME + " or " + PROXY_PASSWORD + " set without the other."
        throw new IllegalArgumentException(msg)
      }
      awsConf.setProxyUsername(proxyUsername)
      awsConf.setProxyPassword(proxyPassword)
      awsConf.setProxyDomain(hadoopConf.getTrimmed(PROXY_DOMAIN))
      awsConf.setProxyWorkstation(hadoopConf.getTrimmed(PROXY_WORKSTATION))
    } else if (proxyPort >= 0) {
      val msg = "Proxy error: " + PROXY_PORT + " set without " + PROXY_HOST
      throw new IllegalArgumentException(msg)
    }

    awsConf
  }

}