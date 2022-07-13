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

import org.apache.spark.sql.{SQLContext, SparkSession, functions}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.sedona_sql.expressions.{ST_GeomFromWKB, ST_GeomFromWKT}

import scala.reflect.runtime.universe.typeTag
import scala.reflect.{ClassTag, classTag}

object UdfRegistrator {

  def registerAll(sqlContext: SQLContext): Unit = {
    registerAll(sqlContext.sparkSession)
  }

  def registerAll(sparkSession: SparkSession): Unit = {
//    Catalog.expressions.foreach(f => {
//      val functionIdentifier = FunctionIdentifier(f.getClass.getSimpleName.dropRight(1))
//      val expressionInfo = new ExpressionInfo(
//        f.getClass.getCanonicalName,
//        functionIdentifier.database.orNull,
//        functionIdentifier.funcName)
//      sparkSession.sessionState.functionRegistry.registerFunction(
//        functionIdentifier,
//        expressionInfo,
//        f
//      )
//    })

    val x = Seq(Int, Long)

    def register(clazz: Class[_]): Any = {
      //val clazz = scala.reflect.classTag[T].runtimeClass
      val name = clazz.getSimpleName.dropRight(1)
      print(name + " " + ClassTag(clazz) + "\n")
      val (expressionInfo, builder) = FunctionRegistryBase.build(name, None)(ClassTag(clazz))
      val newBuilder = (expressions: Seq[Expression]) => {
        builder(expressions)
      }
      sparkSession.sessionState.functionRegistry.registerFunction(FunctionIdentifier(name), expressionInfo, newBuilder)

    }
    Catalog.expressions.foreach(t => {
      register(t.getClass)
    })
    register(classOf[ST_GeomFromWKB])
    register(classOf[ST_GeomFromWKT])
//    register(classOf[ST_GeomFromWKB])
//    register(ST_GeomFromWKT]("ST_GeomFromWKT")

Catalog.aggregateExpressions.foreach(f => sparkSession.udf.register(f.getClass.getSimpleName, functions.udaf(f))) // SPARK3 anchor
//Catalog.aggregateExpressions_UDAF.foreach(f => sparkSession.udf.register(f.getClass.getSimpleName, f)) // SPARK2 anchor
  }

  def dropAll(sparkSession: SparkSession): Unit = {
    Catalog.expressions.foreach(f => sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier(f.getClass.getSimpleName.dropRight(1))))
Catalog.aggregateExpressions.foreach(f => sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier(f.getClass.getSimpleName))) // SPARK3 anchor
//Catalog.aggregateExpressions_UDAF.foreach(f => sparkSession.sessionState.functionRegistry.dropFunction(FunctionIdentifier(f.getClass.getSimpleName))) // SPARK2 anchor
  }
}