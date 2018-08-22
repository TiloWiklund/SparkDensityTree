/**************************************************************************
 * Copyright 2017 Tilo Wiklund
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **************************************************************************/

package co.wiklund.disthist

import org.apache.spark.mllib.linalg.{ Vector => MLVectorImport, _ }

object Types {
  type MLVector = MLVectorImport
  type Axis = Int
  type Intercept = Double
  type Volume = Double
  type Depth = Int
  type Probability = Double
  type Count = Long

  def some[A](a : A) : Option[A] = Some(a)
  def none[A]() : Option[A] = None
}
