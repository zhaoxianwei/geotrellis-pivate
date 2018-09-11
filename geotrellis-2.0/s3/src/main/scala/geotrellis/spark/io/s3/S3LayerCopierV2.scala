package geotrellis.spark.io.s3

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.index.KeyIndex
import geotrellis.spark.io.json._
import geotrellis.util._

import com.amazonaws.services.s3.model.ObjectListing
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import spray.json.JsonFormat

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class S3LayerCopierV2(
  val attributeStore: AttributeStore,
  destBucket: Option[String] = None,
  destKeyPrefix: Option[String] = None,
  s3Client: Option[S3Client] = None) extends LayerCopier[LayerId] {

  def getS3Client: S3Client = s3Client.getOrElse(S3Client.DEFAULT)

  @tailrec
  final def copyListing(s3Client: S3Client, bucket: String, listing: ObjectListing, from: LayerId, to: LayerId): Unit = {

    listing.getObjectSummaries.foreach { os =>
      val key = os.getKey
      getS3Client.copyObject(bucket, key, destBucket.getOrElse(bucket), key.replace(s"${from.name}/${from.zoom}", s"${to.name}/${to.zoom}"))
    }
    if (listing.isTruncated) copyListing(s3Client, bucket, s3Client.listNextBatchOfObjects(listing), from, to)
  }

  def copy[
  K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
  V: AvroRecordCodec: ClassTag,
  M: JsonFormat: Component[?, Bounds[K]]
  ](from: LayerId, to: LayerId): Unit = {
    if (!attributeStore.layerExists(from)) throw new LayerNotFoundError(from)
    if (attributeStore.layerExists(to)) throw new LayerExistsError(to)

    val LayerAttributes(header, metadata, keyIndex, schema) = try {
      attributeStore.readLayerAttributes[S3LayerHeader, M, K](from)
    } catch {
      case e: AttributeNotFoundError => throw new LayerReadError(from).initCause(e)
    }

    val bucket = header.bucket
    val prefix = header.key
    val _s3Client = s3Client.get

    copyListing(_s3Client, bucket, getS3Client.listObjects(bucket, prefix), from, to)
    attributeStore.copy(from, to)
    attributeStore.writeLayerAttributes(
      to, header.copy(
        bucket = destBucket.getOrElse(bucket),
        key    = makePath(destKeyPrefix.getOrElse(prefix), s"${to.name}/${to.zoom}")
      ), metadata, keyIndex, schema
    )
  }
}

object S3LayerCopierV2 {
  def apply(attributeStore: AttributeStore, destBucket: String, destKeyPrefix: String, s3Client: S3Client): S3LayerCopierV2 =
    new S3LayerCopierV2(attributeStore, Some(destBucket), Some(destKeyPrefix), Some(s3Client))

  def apply(attributeStore: AttributeStore, s3Client: S3Client): S3LayerCopierV2 =
    new S3LayerCopierV2(attributeStore, None, None, Some(s3Client))
}


