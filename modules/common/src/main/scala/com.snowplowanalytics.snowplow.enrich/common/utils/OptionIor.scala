/*
 * Copyright (c) 2024-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import cats.Monoid

/**
 * OptionIor is a variant of Ior that can represent non-existence of both A and B.
 * We created this type to be able to represent dropped events as well.
 * OptionIor.Left => bad row
 * OptionIor.Right => enriched event
 * OptionIor.Both => failed event
 * OptionIor.None => dropped event
 */
sealed trait OptionIor[+A, +B] extends Product with Serializable {
  def fold[C](
    fa: A => C,
    fb: B => C,
    fab: (A, B) => C
  )(
    implicit C: Monoid[C]
  ): C =
    this match {
      case OptionIor.Left(a) => fa(a)
      case OptionIor.Right(b) => fb(b)
      case OptionIor.Both(a, b) => fab(a, b)
      case OptionIor.None => Monoid[C].empty
    }

  def bimap[C, D](fa: A => C, fb: B => D): OptionIor[C, D] =
    this match {
      case OptionIor.Left(a) => OptionIor.Left(fa(a))
      case OptionIor.Right(b) => OptionIor.Right(fb(b))
      case OptionIor.Both(a, b) => OptionIor.Both(fa(a), fb(b))
      case OptionIor.None => OptionIor.None
    }

  def map[D](f: B => D): OptionIor[A, D] = bimap(identity, f)

  def leftMap[C](f: A => C): OptionIor[C, B] = bimap(f, identity)

  def merge[AA >: A](implicit ev: B <:< AA, AA: Monoid[AA]): AA =
    fold(identity, ev, (a, b) => AA.combine(a, b))
}

object OptionIor {
  final case class Left[+A](a: A) extends OptionIor[A, Nothing]
  final case class Right[+B](b: B) extends OptionIor[Nothing, B]
  final case class Both[+A, +B](a: A, b: B) extends OptionIor[A, B]
  final case object None extends OptionIor[Nothing, Nothing]
}
