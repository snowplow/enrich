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

import cats.data.{Ior, IorT}
import cats.{Functor, Monad, Semigroup}
import cats.implicits._
import com.snowplowanalytics.snowplow.enrich.common.utils.OptionIor.{Both, Left, Right}

final case class OptionIorT[F[_], A, B](value: F[OptionIor[A, B]]) {
  def flatMap[AA >: A, D](f: B => OptionIorT[F, AA, D])(implicit F: Monad[F], AA: Semigroup[AA]): OptionIorT[F, AA, D] =
    OptionIorT {
      F.flatMap(value) {
        case l @ OptionIor.Left(_) => F.pure(l.asInstanceOf[OptionIor[AA, D]])
        case OptionIor.Right(b) => f(b).value
        case OptionIor.None => F.pure(OptionIor.None)
        case OptionIor.Both(a, b) =>
          F.map(f(b).value) {
            case OptionIor.Left(a1) => OptionIor.Left(AA.combine(a, a1))
            case OptionIor.Right(d) => OptionIor.Both(a, d)
            case OptionIor.Both(a1, d) => OptionIor.Both(AA.combine(a, a1), d)
            case OptionIor.None => OptionIor.None
          }
      }
    }

  def map[D](f: B => D)(implicit F: Functor[F]): OptionIorT[F, A, D] =
    OptionIorT(F.map(value)(_.map(f)))

  def leftMap[C](f: A => C)(implicit F: Functor[F]): OptionIorT[F, C, B] =
    OptionIorT(F.map(value)(_.leftMap(f)))
}

object OptionIorT {
  implicit class EnrichStateTOps[F[_], A, B](val iorT: IorT[F, A, B]) extends AnyVal {
    def toOptionIorT(implicit F: Functor[F]): OptionIorT[F, A, B] =
      OptionIorT {
        iorT.value.map {
          case Ior.Left(a) => Left(a)
          case Ior.Right(b) => Right(b)
          case Ior.Both(a, b) => Both(a, b)
        }
      }
  }
}
