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

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth._
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model._
import org.apache.commons.io.IOUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

object AmazonS3Client {

  val defaultConfig = new com.amazonaws.ClientConfiguration
  defaultConfig.setMaxConnections(128)
  defaultConfig.setMaxErrorRetry(16)
  defaultConfig.setConnectionTimeout(100000)
  defaultConfig.setSocketTimeout(100000)
  defaultConfig.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(32))


  def apply(): AmazonS3Client = {
    val s3Client = new com.amazonaws.services.s3.AmazonS3Client(new DefaultAWSCredentialsProviderChain(), defaultConfig)
    AmazonS3Client(s3Client)
  }

  def apply(s3client: AmazonS3): AmazonS3Client =
    new AmazonS3Client(s3client)

  def apply(provider: AWSCredentialsProvider, config: ClientConfiguration): AmazonS3Client = {
    val s3Client = new com.amazonaws.services.s3.AmazonS3Client(new DefaultAWSCredentialsProviderChain(), config)
    AmazonS3Client(s3Client)
  }


  def apply(provider: AWSCredentialsProvider): AmazonS3Client =
    apply(provider, new ClientConfiguration())
}

class AmazonS3Client(s3client: AmazonS3) extends S3Client {
  def doesBucketExist(bucket: String): Boolean = s3client.doesBucketExist(bucket)

  def doesObjectExist(bucket: String, key: String): Boolean = {
    import com.amazonaws.services.s3.model.AmazonS3Exception
    try {
      getObjectMetadata(bucket, key)
      return true
    } catch {
      case e: AmazonS3Exception =>
        if (e.getStatusCode == 404) return false
        throw e
    }
  }

  def listObjects(listObjectsRequest: ListObjectsRequest): ObjectListing =
    s3client.listObjects(listObjectsRequest)

  def listKeys(listObjectsRequest: ListObjectsRequest): Seq[String] = {
    var listing: ObjectListing = null
    val result = mutable.ListBuffer[String]()
    do {
      listing = s3client.listObjects(listObjectsRequest)
      // avoid including "directories" in the input split, can cause 403 errors on GET
      result ++= listing.getObjectSummaries.asScala.map(_.getKey).filterNot(_ endsWith "/")
      listObjectsRequest.setMarker(listing.getNextMarker)
    } while (listing.isTruncated)

    result
  }

  def getObject(getObjectRequest: GetObjectRequest): S3Object =
    s3client.getObject(getObjectRequest)

  def putObject(putObjectRequest: PutObjectRequest): PutObjectResult =
    s3client.putObject(putObjectRequest)

  def deleteObject(deleteObjectRequest: DeleteObjectRequest): Unit =
    s3client.deleteObject(deleteObjectRequest)

  def copyObject(copyObjectRequest: CopyObjectRequest): CopyObjectResult =
    s3client.copyObject(copyObjectRequest)

  def listNextBatchOfObjects(listing: ObjectListing): ObjectListing =
    s3client.listNextBatchOfObjects(listing)

  def deleteObjects(deleteObjectsRequest: DeleteObjectsRequest): Unit =
    s3client.deleteObjects(deleteObjectsRequest)

  def readBytes(getObjectRequest: GetObjectRequest): Array[Byte] = {
    val obj = s3client.getObject(getObjectRequest)
    val inStream = obj.getObjectContent
    try {
      IOUtils.toByteArray(inStream)
    } finally {
      inStream.close()
    }
  }

  def readRange(start: Long, end: Long, getObjectRequest: GetObjectRequest): Array[Byte] = {
    getObjectRequest.setRange(start, end - 1)
    val obj = s3client.getObject(getObjectRequest)
    val stream = obj.getObjectContent
    try {
      IOUtils.toByteArray(stream)
    } finally {
      stream.close()
    }
  }

  def getObjectMetadata(getObjectMetadataRequest: GetObjectMetadataRequest): ObjectMetadata =
    s3client.getObjectMetadata(getObjectMetadataRequest)

  def setRegion(region: com.amazonaws.regions.Region): Unit = {
    s3client.setRegion(region)
  }
}
