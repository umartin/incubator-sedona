/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.core.spatialOperator.SpatialPredicate
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sedona_sql.execution.SedonaBinaryExecNode
import org.locationtech.jts.geom.Geometry

/**
 * Distance joins requires matching geometries to be in the same partition, despite not necessarily overlapping.
 * To create an overlap and guarantee matching geometries end up in the same partition, the left geometry is expanded
 * before partitioning. It's the logical equivalent of:
 *
 * select * from a join b on ST_Distance(a.geom, b.geom) <= 1
 *
 * becomes
 *
 * select * from a join b on ST_Intersects(ST_Envelope(ST_Buffer(a.geom, 1)), b.geom) and ST_Distance(a.geom, b.geom) <= 1
 *
 * @param left left side of the join
 * @param right right side of the join
 * @param leftShape expression for the first argument of spatialPredicate
 * @param rightShape expression for the second argument of spatialPredicate
 * @param swappedLeftAndRight boolean indicating whether left and right plans were swapped
 * @param distance - ST_Distance(left, right) <= distance. Distance can be literal or a computation over 'left'.
 * @param spatialPredicate spatial predicate as join condition
 * @param extraCondition extra join condition other than spatialPredicate
 */
case class DistanceJoinExec(left: SparkPlan,
                            right: SparkPlan,
                            leftShape: Expression,
                            rightShape: Expression,
                            distance: Expression,
                            spatialPredicate: SpatialPredicate,
                            extraCondition: Option[Expression] = None)
  extends SedonaBinaryExecNode
    with TraitJoinQueryExec
    with Logging {

  override def toSpatialRddPair(
                                 buildRdd: RDD[UnsafeRow],
                                 buildExpr: Expression,
                                 streamedRdd: RDD[UnsafeRow],
                                 streamedExpr: Expression): (SpatialRDD[Geometry], SpatialRDD[Geometry]) =
    if (distance.references.isEmpty || distance.references.forall(left.output.contains(_))) {
      (toExpandedEnvelopeRDD(buildRdd, buildExpr, BindReferences.bindReference(distance, left.output)), toSpatialRDD(streamedRdd, streamedExpr))
    } else {
      (toSpatialRDD(streamedRdd, streamedExpr), toExpandedEnvelopeRDD(buildRdd, buildExpr, BindReferences.bindReference(distance, right.output)))
    }

  protected def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }

}
