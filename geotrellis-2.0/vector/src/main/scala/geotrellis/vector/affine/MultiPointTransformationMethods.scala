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

package geotrellis.vector.affine

import geotrellis.util.MethodExtensions
import geotrellis.vector._

/** Trait used to implicitly extend [[MultiPoint]] instances with transformation methods */
trait MultiPointTransformationMethods extends MethodExtensions[MultiPoint] {

  /**
    * Transform according to a provided AffineTransformation instance.
    *
    * @param  trans  An AffineTransformation
    */
  def transform(trans: AffineTransformation) = trans.transform(self)

  /** Reflect over the provided point */
  def reflect(p: Point): MultiPoint =
    reflect(p.x, p.y)

  /** Reflect over the provided point */
  def reflect(x: Double, y: Double): MultiPoint =
    Reflection(x, y).transform(self)

  /** Reflect over the line between two points */
  def reflect(p0: Point, p1: Point): MultiPoint =
    reflect(p0.x, p0.y, p1.x, p1.y)

  /** Reflect over the line between two points */
  def reflect(x0: Double, y0: Double, x1: Double, y1: Double): MultiPoint =
    Reflection(x0, y0, x1, y1).transform(self)

  /** Rotate about its origin by the specified radians
    * @param theta  The number of radians about the origin to rotate this multipoint. Positive
    *               numbers correspond to counter-clockwise rotation
    */
  def rotate(theta: Double): MultiPoint =
    Rotation(theta).transform(self)

  /** Rotate about its origin by the specified angle
    * @param sinTheta  The sin of the angle angle about the origin to rotate this multipoint
    * @param cosTheta  The cosin of the angle about the origin to rotate this polymultipoint
    */
  def rotate(sinTheta: Double, cosTheta: Double): MultiPoint =
    Rotation(sinTheta, cosTheta).transform(self)

  /** Change the scale of this multipoint
    * @param xscale  the updated scale on the x-axis
    * @param yscale  the updated scale on the y-axis
    */
  def scale(xscale: Double, yscale: Double): MultiPoint =
    Scaling(xscale, yscale).transform(self)

  /** Shear the provided multipoint
    * @param xshear  Shear on the x axis
    * @param yshear  Shear on the y axis
    */
  def shear(xshear: Double, yshear: Double): MultiPoint =
    Shearing(xshear, yshear).transform(self)

  def translate(x: Double, y: Double): MultiPoint =
    Translation(x, y).transform(self)
}
