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
package org.apache.sedona.sql.UDF
import scala.reflect.runtime.universe._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedAggregateFunction}
import org.apache.spark.sql.sedona_sql.expressions.{ST_YMax, ST_YMin, _}
import org.apache.spark.sql.sedona_sql.expressions.collect.{ST_Collect, ST_CollectionExtract}
import org.apache.spark.sql.sedona_sql.expressions.raster.{RS_Add, RS_Append, RS_Array, RS_Base64, RS_BitwiseAnd, RS_BitwiseOr, RS_Count, RS_Divide, RS_FetchRegion, RS_GetBand, RS_GreaterThan, RS_GreaterThanEqual, RS_HTML, RS_LessThan, RS_LessThanEqual, RS_LogicalDifference, RS_LogicalOver, RS_Mean, RS_Mode, RS_Modulo, RS_Multiply, RS_MultiplyFactor, RS_Normalize, RS_NormalizedDifference, RS_SquareRoot, RS_Subtract}
import org.locationtech.jts.geom.Geometry

object Catalog {
  val expressions = Seq(
    // Expression for vectors
    ST_PointFromText,
    ST_PolygonFromText,
    ST_LineStringFromText,
    ST_GeomFromText,
    ST_LineFromText,
//    ST_GeomFromWKT,
//    ST_GeomFromWKB,
    ST_GeomFromGeoJSON,
    ST_Point,
    ST_PolygonFromEnvelope,
    ST_Contains,
    ST_Intersects,
    ST_Within,
    ST_Disjoint,
    ST_Distance,
    ST_3DDistance,
    ST_ConvexHull,
    ST_NPoints,
    ST_Buffer,
    ST_Envelope,
    ST_Length,
    ST_Area,
    ST_Centroid,
    ST_Transform,
    ST_Intersection,
    ST_Difference,
    ST_SymDifference,
    ST_Union,
    ST_IsValid,
    ST_IsEmpty,
    ST_PrecisionReduce,
    ST_Equals,
    ST_Touches,
    ST_Overlaps,
    ST_Crosses,
    ST_IsSimple,
    ST_MakeValid,
    ST_SimplifyPreserveTopology,
    ST_AsText,
    ST_AsGeoJSON,
    ST_AsBinary,
    ST_AsEWKB,
    ST_SRID,
    ST_SetSRID,
    ST_GeometryType,
    ST_NumGeometries,
    ST_LineMerge,
    ST_Azimuth,
    ST_X,
    ST_Y,
    ST_Z,
    ST_StartPoint,
    ST_Boundary,
    ST_MinimumBoundingRadius,
    ST_MinimumBoundingCircle,
    ST_EndPoint,
    ST_ExteriorRing,
    ST_GeometryN,
    ST_InteriorRingN,
    ST_Dump,
    ST_DumpPoints,
    ST_IsClosed,
    ST_NumInteriorRings,
    ST_AddPoint,
    ST_RemovePoint,
    ST_IsRing,
    ST_FlipCoordinates,
    ST_LineSubstring,
    ST_LineInterpolatePoint,
    ST_SubDivideExplode,
    ST_SubDivide,
    ST_MakePolygon,
    ST_GeoHash,
    ST_GeomFromGeoHash,
    ST_Collect,
    ST_Multi,
    ST_PointOnSurface,
    ST_Reverse,
    ST_PointN,
    ST_AsEWKT,
    ST_Force_2D,
    ST_YMax,
    ST_YMin,
    ST_XMax,
    ST_XMin,
    ST_BuildArea,
    ST_OrderingEquals,
    ST_CollectionExtract,
    // Expression for rasters
    RS_NormalizedDifference,
    RS_Mean,
    RS_Mode,
    RS_FetchRegion,
    RS_GreaterThan,
    RS_GreaterThanEqual,
    RS_LessThan,
    RS_LessThanEqual,
    RS_Add,
    RS_Subtract,
    RS_Divide,
    RS_MultiplyFactor,
    RS_Multiply,
    RS_BitwiseAnd,
    RS_BitwiseOr,
    RS_Count,
    RS_Modulo,
    RS_GetBand,
    RS_SquareRoot,
    RS_LogicalDifference,
    RS_LogicalOver,
    RS_Base64,
    RS_HTML,
    RS_Array,
    RS_Normalize,
    RS_Append
  )

  val aggregateExpressions: Seq[Aggregator[Geometry, Geometry, Geometry]] = Seq(
    new ST_Union_Aggr,
    new ST_Envelope_Aggr,
    new ST_Intersection_Aggr
  )

  import org.apache.spark.sql.sedona_sql.expressions_udaf
  val aggregateExpressions_UDAF: Seq[UserDefinedAggregateFunction] = Seq(
    new expressions_udaf.ST_Union_Aggr,
    new expressions_udaf.ST_Envelope_Aggr,
    new expressions_udaf.ST_Intersection_Aggr
  )
}
