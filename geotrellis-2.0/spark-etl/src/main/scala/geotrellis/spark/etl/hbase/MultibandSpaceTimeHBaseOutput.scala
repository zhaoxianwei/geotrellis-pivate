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

package geotrellis.spark.etl.hbase

import geotrellis.raster.MultibandTile
import geotrellis.spark._
import geotrellis.spark.etl.config.EtlConf
import geotrellis.spark.io._
import geotrellis.spark.io.hbase.HBaseLayerWriter

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class MultibandSpaceTimeHBaseOutput extends HBaseOutput[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] {
  def writer(conf: EtlConf)(implicit sc: SparkContext) =
    HBaseLayerWriter(getInstance(conf.outputProfile), getPath(conf.output.backend).table).writer[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](conf.output.getKeyIndexMethod[SpaceTimeKey])

  def update(id: LayerId, rdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], conf: EtlConf)(implicit sc: SparkContext): Unit = {
    HBaseLayerWriter(getInstance(conf.outputProfile), getPath(conf.output.backend).table).update[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](id, rdd, mergeFunc={ (existing: MultibandTile, updating: MultibandTile) => existing.merge(updating) })
  }
}
