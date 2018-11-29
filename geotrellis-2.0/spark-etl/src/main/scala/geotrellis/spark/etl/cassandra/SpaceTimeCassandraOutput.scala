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

package geotrellis.spark.etl.cassandra

import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.etl.config.EtlConf
import geotrellis.spark.io._
import geotrellis.spark.io.cassandra.CassandraLayerWriter

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class SpaceTimeCassandraOutput extends CassandraOutput[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]] {
  def writer(conf: EtlConf)(implicit sc: SparkContext) = {
    val path = getPath(conf.output.backend)
    CassandraLayerWriter(getInstance(conf.outputProfile), path.keyspace, path.table).writer[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](conf.output.getKeyIndexMethod[SpaceTimeKey])
  }

  def update(id: LayerId, rdd: RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], conf: EtlConf)(implicit sc: SparkContext): Unit = {
    val path = getPath(conf.output.backend)
    CassandraLayerWriter(getInstance(conf.outputProfile), path.keyspace, path.table).update[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](id,  rdd, mergeFunc={ (existing: Tile, updating: Tile) => existing.merge(updating) })
  }
}
