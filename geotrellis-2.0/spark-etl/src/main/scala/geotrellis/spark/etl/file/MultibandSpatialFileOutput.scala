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

package geotrellis.spark.etl.file

import geotrellis.raster.MultibandTile
import geotrellis.spark._
import geotrellis.spark.etl.config.EtlConf
import geotrellis.spark.io._
import geotrellis.spark.io.file._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class MultibandSpatialFileOutput extends FileOutput[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] {
  def writer(conf: EtlConf)(implicit sc: SparkContext) =
    FileLayerWriter(getPath(conf.output.backend).path).writer[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](conf.output.getKeyIndexMethod[SpatialKey])

  def update(id: LayerId, rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], conf: EtlConf)(implicit sc: SparkContext): Unit = {
    FileLayerWriter(getPath(conf.output.backend).path).update[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](id, rdd, mergeFunc={ (existing: MultibandTile, updating: MultibandTile) => existing.merge(updating) })
  }
}
