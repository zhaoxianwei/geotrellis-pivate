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

package geotrellis.spark.etl

import geotrellis.spark.etl.config.EtlConf
import geotrellis.spark.{LayerId, Metadata}
import geotrellis.spark.io.{AttributeStore, Writer, LayerWriter}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait OutputPlugin[K, V, M] extends Plugin {
  import Etl.SaveAction
  //import Etl.UpdateAction

  def name: String

  def attributes(conf: EtlConf): AttributeStore

  def writer(conf: EtlConf)(implicit sc: SparkContext): Writer[LayerId, RDD[(K, V)] with Metadata[M]]

  //def layerwriter(conf: EtlConf)(implicit sc: SparkContext): LayerWriter[LayerId]
  def update(id: LayerId, rdd: RDD[(K, V)] with Metadata[M], conf: EtlConf)(implicit sc: SparkContext): Unit

  def apply(
    id: LayerId,
    rdd: RDD[(K, V)] with Metadata[M],
    conf: EtlConf,
    saveAction: SaveAction[K, V, M] = SaveAction.DEFAULT[K, V, M]
  ): Unit = {
    implicit val sc = rdd.sparkContext
    saveAction(attributes(conf), writer(conf), id, rdd)
  }

  def apply(
    id: LayerId,
    rdd: RDD[(K, V)] with Metadata[M],
    conf: EtlConf
  ): Unit = {
    implicit val sc = rdd.sparkContext
    update(id, rdd, conf)
    //updateAction(attributes(conf), layerwriter(conf), id, rdd)
  }

  def suitableFor(name: String): Boolean =
    name.toLowerCase == this.name
}
