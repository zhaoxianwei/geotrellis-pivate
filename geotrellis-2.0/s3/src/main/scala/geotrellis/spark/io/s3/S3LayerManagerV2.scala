package geotrellis.spark.io.s3

import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.index.{KeyIndex, KeyIndexMethod}
import geotrellis.spark.{Boundable, Bounds, LayerId}
import geotrellis.spark.io.{AttributeStore, LayerManager}
import geotrellis.util.Component
import org.apache.spark.SparkContext
import spray.json.JsonFormat

import scala.reflect.ClassTag

// Since S3LayerManager and S3AttributeStore are tightly coupled, we use S3LayerManagerV2 to decouple
// LayerManager and AttributeStore
class S3LayerManagerV2(
  attributeStore: AttributeStore,
  bucket: String,
  prefix: String,
  getS3Client: () => S3Client = () => S3Client.DEFAULT)
  (implicit sc: SparkContext) extends LayerManager[LayerId] {

  def delete(id: LayerId): Unit =
    S3LayerDeleter(attributeStore).delete(id)

  def copy[
  K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
  V: AvroRecordCodec: ClassTag,
  M: JsonFormat: Component[?, Bounds[K]]
  ](from: LayerId, to: LayerId): Unit =
    S3LayerCopierV2(attributeStore, getS3Client.apply).copy[K, V, M](from, to)

  def move[
  K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
  V: AvroRecordCodec: ClassTag,
  M: JsonFormat: Component[?, Bounds[K]]
  ](from: LayerId, to: LayerId): Unit =
    S3LayerMover(attributeStore, getS3Client.apply).move[K, V, M](from, to)

  def reindex[
  K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
  V: AvroRecordCodec: ClassTag,
  M: JsonFormat: Component[?, Bounds[K]]
  ](id: LayerId, keyIndexMethod: KeyIndexMethod[K]): Unit =
    S3LayerReindexer(attributeStore, bucket, prefix, getS3Client.apply).reindex[K, V, M](id, keyIndexMethod)

  def reindex[
  K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
  V: AvroRecordCodec: ClassTag,
  M: JsonFormat: Component[?, Bounds[K]]
  ](id: LayerId, keyIndex: KeyIndex[K]): Unit =
    S3LayerReindexer(attributeStore, bucket, prefix, getS3Client.apply).reindex[K, V, M](id, keyIndex)

}
