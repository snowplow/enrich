/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import java.lang.reflect.Method

import cats.data.ValidatedNel
import cats.instances.int._
import cats.syntax.either._
import cats.syntax.validated._

/**
 * The problem we're trying to solve: converting maps to classes in Scala
 * is not very easy to do in a functional way, and it gets even harder
 * if you have a class with >22 fields (so can't use case classes).
 *
 * For a discussion about this on Stack Overflow, see:
 * http://stackoverflow.com/questions/4290955/instantiating-a-case-class-from-a-list-of-parameters
 *
 * The idea is to use Java Reflection with a big ol' TransformMap:
 *
 * ("key in map"  -> Tuple2(transformFunc, "field in class"),
 *  "another key" -> Tuple2(transformFunc, "field in class"),
 *  "a third key" -> Tuple2(transformFunc, "field in class"))
 *
 * And then there would be an iteration through the source map which
 * looks up the key in the TransformMap and then applies the
 * transformFunc using Java Reflection, storing the answer in the
 * class's specified field.
 *
 * If I can get this working, then the next step is to generate
 * a "results" map which contains the results of each
 * transformFunc. Each transformFunc should either return a
 * ValidationNel[String, Unit], or should be promoted (via an
 * implicit) to the same.
 *
 * Having this should allow me to do something like:
 * resultsMap.foldLeft(Unit.success, |@|) to roll up any validation
 * errors into one final ValidatioNel.
 *
 * If I can get all that working, then the final step is to
 * support transformFuncs which set multiple fields. To avoid the
 * complexity spiralling, it would probably be simplest if any
 * transformFunc that wanted to return multiple values returned
 * a TupleN, and then we use the same TupleN for the target fields.
 * Maybe there will be an implicit to convert "raw" target fields
 * into Tuple1s.
 *
 * Okay, let's start...
 */
object MapTransformer {

  // Clarificatory aliases
  type Key = String
  type Value = String
  type Field = String

  // A transformation takes a Key and Value and returns either a failure or anything
  type TransformFunc = Function2[Key, Value, Either[AtomicError.ParseError, _]]

  // Our source map
  type SourceMap = Map[Key, Value]

  // Our map for transforming data
  type TransformMap = Map[Key, Tuple2[TransformFunc, _]]

  // All of the setter methods on this object
  type SettersMap = Map[Key, Method]

  /**
   * A factory to generate a new object using a TransformMap.
   * @param sourceMap Contains the source data to apply to the obj
   * @param transformMap Determines how the data should be transformed before storing in the obj
   * @return a ValidationNel containing either a Nel of error Strings, or the new object
   */
  def generate[T <: AnyRef](
    sourceMap: SourceMap,
    transformMap: TransformMap
  )(
    implicit m: Manifest[T]
  ): ValidatedNel[AtomicError.ParseError, T] = {
    val newInst = m.runtimeClass.getDeclaredConstructor().newInstance()
    val result = _transform(newInst, sourceMap, transformMap, getSetters(m.runtimeClass))
    // On success, replace the field count with the new instance
    result.map(_ => newInst.asInstanceOf[T])
  }

  /**
   * An implicit conversion to take any Object and make it Transformable.
   * @param obj Any Object
   * @return the new Transformable class, with manifest attached
   */
  implicit def makeTransformable[T <: AnyRef](obj: T)(implicit m: Manifest[T]) =
    new TransformableClass[T](obj)

  /** A pimped object, now transformable by using the transform method. */
  class TransformableClass[T](obj: T)(implicit m: Manifest[T]) {

    // Do all the reflection for the setters we need:
    // This needs to be lazy because Method is not serializable
    private lazy val setters = getSetters(m.runtimeClass)

    /**
     * Update the object by applying the contents of a SourceMap to the object using a TransformMap.
     * @param sourceMap Contains the source data to apply to the obj
     * @param transformMap Determines how the data should be transformed before storing in the obj
     * @return a ValidationNel containing a Nel of error Strings, or the count of updated fields
     */
    def transform(sourceMap: SourceMap, transformMap: TransformMap): ValidatedNel[AtomicError.ParseError, Int] =
      _transform[T](obj, sourceMap, transformMap, setters)
  }

  /**
   * General-purpose method to update any object by applying the contents of a SourceMap to
   * the object using a TransformMap. We use the SettersMap to update the object.
   * @param obj Any Object
   * @param sourceMap Contains the source data to apply to the obj
   * @param transformMap Determines how the data should be transformed before storing in the obj
   * @param setters Provides access to the obj's setX() methods
   * @return a ValidationNel containing a Nel of error Strings, or the count of updated fields
   */
  private def _transform[T](
    obj: T,
    sourceMap: SourceMap,
    transformMap: TransformMap,
    setters: SettersMap
  ): ValidatedNel[AtomicError.ParseError, Int] = {
    val results: List[Either[AtomicError.ParseError, Int]] = sourceMap.map {
      case (key, in) =>
        transformMap.get(key) match {
          case Some((func, field)) =>
            func(key, in) match {
              //weird issue with type inference when using map
              case Left(e) => e.asLeft[Int]
              case Right(s) =>
                field match {
                  case f: String =>
                    val result = s.asInstanceOf[AnyRef]
                    setters(f).invoke(obj, result)
                    1.asRight // +1 to the count of fields successfully set
                  case Tuple2(f1: String, f2: String) =>
                    val result = s.asInstanceOf[Tuple2[AnyRef, AnyRef]]
                    setters(f1).invoke(obj, result._1)
                    setters(f2).invoke(obj, result._2)
                    2.asRight // +2 to the count of fields successfully set
                  case Tuple3(f1: String, f2: String, f3: String) =>
                    val result = s.asInstanceOf[Tuple3[AnyRef, AnyRef, AnyRef]]
                    setters(f1).invoke(obj, result._1)
                    setters(f2).invoke(obj, result._2)
                    setters(f3).invoke(obj, result._3)
                    3.asRight // +3 to the count of fields successfully set
                  case Tuple4(f1: String, f2: String, f3: String, f4: String) =>
                    val result = s.asInstanceOf[Tuple4[AnyRef, AnyRef, AnyRef, AnyRef]]
                    setters(f1).invoke(obj, result._1)
                    setters(f2).invoke(obj, result._2)
                    setters(f3).invoke(obj, result._3)
                    setters(f4).invoke(obj, result._4)
                    4.asRight // +4 to the count of fields successfully set
                }
            }
          case None => 0.asRight // Key not found: zero fields updated
        }
    }.toList

    results.foldLeft(0.validNel[AtomicError.ParseError]) {
      case (acc, e) =>
        acc.combine(e.toValidatedNel)
    }
  }

  /**
   * Lowercases the first character in a String.
   * @param s The String to lowercase the first letter of
   * @return s with the first character in lowercase
   */
  private def lowerFirst(s: String): String =
    s.substring(0, 1).toLowerCase + s.substring(1)

  /**
   * Gets the field name from a setter Method, by cutting out "set" and lowercasing the first
   * character after in the setter's name.
   * @param setter The Method from which we will reverse-engineer the field name
   * @return the field name extracted from the setter
   */
  private def setterToFieldName(setter: Method): String =
    lowerFirst(setter.getName.substring(3))

  /**
   * Gets all of the setter Methods from a manifest.
   * @param c The manifest containing the setter methods to return
   * @return the Map of setter Methods
   */
  private def getSetters[T](c: Class[T]): SettersMap =
    c.getDeclaredMethods
      .filter(_.getName.startsWith("set"))
      .groupBy(setterToFieldName(_))
      .mapValues(_.head)
}
