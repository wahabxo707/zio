/*
 * Copyright 2019-2024 John A. De Goes and the ZIO Contributors
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

package zio.test

import zio.Random._
import zio.stacktracer.TracingImplicits.disableAutoTrace
import zio.stream.ZStream
import zio.{Chunk, NonEmptyChunk, Random, Trace, UIO, URIO, ZIO, Zippable}

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters._
import scala.math.Numeric.DoubleIsFractional

/**
 * A `Gen[R, A]` represents a generator of values of type `A`, which requires an
 * environment `R`. Generators may be random or deterministic.
 */
final case class Gen[-R, +A](sample: ZStream[R, Nothing, Sample[R, A]]) { self =>

  /**
   * A symbolic alias for `concat`.
   */
  def ++[R1 <: R, A1 >: A](that: Gen[R1, A1])(implicit trace: Trace): Gen[R1, A1] =
    self.concat(that)

  /**
   * A symbolic alias for `zip`.
   */
  def <*>[R1 <: R, B](
    that: Gen[R1, B]
  )(implicit zippable: Zippable[A, B], trace: Trace): Gen[R1, zippable.Out] =
    self.zip(that)

  /**
   * Concatenates the specified deterministic generator with this determinstic
   * generator, resulting in a deterministic generator that generates the values
   * from this generator and then the values from the specified generator.
   */
  def concat[R1 <: R, A1 >: A](that: Gen[R1, A1])(implicit trace: Trace): Gen[R1, A1] =
    Gen(self.sample ++ that.sample)

  /**
   * Maps the values produced by this generator with the specified partial
   * function, discarding any values the partial function is not defined at.
   */
  def collect[B](pf: PartialFunction[A, B])(implicit trace: Trace): Gen[R, B] =
    self.flatMap { a =>
      pf.andThen(Gen.const(_)).applyOrElse[A, Gen[Any, B]](a, _ => Gen.empty)
    }

  /**
   * Filters the values produced by this generator, discarding any values that
   * do not meet the specified predicate. Using `filter` can reduce test
   * performance, especially if many values must be discarded. It is recommended
   * to use combinators such as `map` and `flatMap` to create generators of the
   * desired values instead.
   *
   * {{{
   * val evens: Gen[Any, Int] = Gen.int.map(_ * 2)
   * }}}
   */
  def filter(f: A => Boolean)(implicit trace: Trace): Gen[R, A] =
    self.flatMap(a => if (f(a)) Gen.const(a) else Gen.empty)

  /**
   * Filters the values produced by this generator, discarding any values that
   * do not meet the specified effectual predicate. Using `filterZIO` can reduce
   * test performance, especially if many values must be discarded. It is
   * recommended to use combinators such as `map` and `flatMap` to create
   * generators of the desired values instead.
   *
   * {{{
   * val evens: Gen[Any, Int] = Gen.int.map(_ * 2)
   * }}}
   */
  def filterZIO[R1 <: R](f: A => ZIO[R1, Nothing, Boolean])(implicit trace: Trace): Gen[R1, A] =
    self.flatMap(a => Gen.fromZIO(f(a)).flatMap(p => if (p) Gen.const(a) else Gen.empty))

  /**
   * Filters the values produced by this generator, discarding any values that
   * meet the specified predicate.
   */
  def filterNot(f: A => Boolean)(implicit trace: Trace): Gen[R, A] =
    filter(a => !f(a))

  def withFilter(f: A => Boolean)(implicit trace: Trace): Gen[R, A] = filter(f)

  def flatMap[R1 <: R, B](f: A => Gen[R1, B])(implicit trace: Trace): Gen[R1, B] =
    Gen {
      self.sample.flatMap { sample =>
        val values  = f(sample.value).sample
        val shrinks = Gen(sample.shrink).flatMap(f).sample
        values.map(_.flatMap(Sample(_, shrinks)))
      }
    }

  def flatten[R1 <: R, B](implicit ev: A <:< Gen[R1, B], trace: Trace): Gen[R1, B] =
    flatMap(ev)

  def map[B](f: A => B)(implicit trace: Trace): Gen[R, B] =
    Gen(sample.map(_.map(f)))

  /**
   * Maps an effectual function over a generator.
   */
  def mapZIO[R1 <: R, B](f: A => ZIO[R1, Nothing, B])(implicit trace: Trace): Gen[R1, B] =
    Gen(sample.mapZIO(_.foreach(f)))

  /**
   * Discards the shrinker for this generator.
   */
  def noShrink(implicit trace: Trace): Gen[R, A] =
    reshrink(Sample.noShrink)

  /**
   * Discards the shrinker for this generator and applies a new shrinker by
   * mapping each value to a sample using the specified function. This is useful
   * when the process to shrink a value is simpler than the process used to
   * generate it.
   */
  def reshrink[R1 <: R, B](f: A => Sample[R1, B])(implicit trace: Trace): Gen[R1, B] =
    Gen(sample.map(sample => f(sample.value)))

  /**
   * Sets the size parameter for this generator to the specified value.
   */
  def resize(size: Int)(implicit trace: Trace): Gen[R, A] =
    Sized.withSizeGen(size)(self)

  /**
   * Runs the generator and collects all of its values in a list.
   */
  def runCollect(implicit trace: Trace): ZIO[R, Nothing, List[A]] =
    sample.map(_.value).runCollect.map(_.toList)

  /**
   * Repeatedly runs the generator and collects the specified number of values
   * in a list.
   */
  def runCollectN(n: Int)(implicit trace: Trace): ZIO[R, Nothing, List[A]] =
    sampleN(n).map(_.value).runCollect.map(_.toList)

  /**
   * Runs the generator returning the first value of the generator.
   */
  def runHead(implicit trace: Trace): ZIO[R, Nothing, Option[A]] =
    sample.map(_.value).runHead

  private[test] def sampleN(n: Int)(implicit trace: Trace): ZStream[R, Nothing, Sample[R, A]] =
    ZStream.repeatZIOOption(sample.runHead.some).take(n.toLong)

  private[test] def sampleValuesN(n: Int)(implicit trace: Trace): ZStream[R, Nothing, A] =
    sampleN(n).map(_.value)

  /**
   * Composes this generator with the specified generator to create a cartesian
   * product of elements.
   */
  def zip[R1 <: R, B](
    that: Gen[R1, B]
  )(implicit zippable: Zippable[A, B], trace: Trace): Gen[R1, zippable.Out] =
    self.zipWith(that)(zippable.zip(_, _))

  /**
   * Composes this generator with the specified generator to create a cartesian
   * product of elements with the specified function.
   */
  def zipWith[R1 <: R, B, C](that: Gen[R1, B])(f: (A, B) => C)(implicit trace: Trace): Gen[R1, C] =
    self.flatMap(a => that.map(b => f(a, b)))
}

object Gen extends GenZIO with FunctionVariants with TimeVariants {

  /**
   * A generator of alpha characters.
   */
  def alphaChar(implicit trace: Trace): Gen[Any, Char] =
    weighted(char(65, 90) -> 26, char(97, 122) -> 26)

  /**
   * A generator of alphanumeric characters. Shrinks toward '0'.
   */
  def alphaNumericChar(implicit trace: Trace): Gen[Any, Char] =
    weighted(char(48, 57) -> 10, char(65, 90) -> 26, char(97, 122) -> 26)

  /**
   * A generator of alphanumeric strings. Shrinks towards the empty string.
   */
  def alphaNumericString(implicit trace: Trace): Gen[Any, String] =
    Gen.string(alphaNumericChar)

  /**
   * A generator of alphanumeric strings whose size falls within the specified
   * bounds.
   */
  def alphaNumericStringBounded(min: Int, max: Int)(implicit
    trace: Trace
  ): Gen[Any, String] =
    Gen.stringBounded(min, max)(alphaNumericChar)

  /**
   * A generator of US-ASCII characters. Shrinks toward '0'.
   */
  def asciiChar(implicit trace: Trace): Gen[Any, Char] =
    Gen.oneOf(Gen.char('\u0000', '\u007F'))

  /**
   * A generator US-ASCII strings. Shrinks towards the empty string.
   */
  def asciiString(implicit trace: Trace): Gen[Any, String] =
    Gen.string(Gen.asciiChar)

  /**
   * A generator of big decimals inside the specified range: [start, end]. The
   * shrinker will shrink toward the lower end of the range ("smallest").
   *
   * The values generated will have a precision equal to the precision of the
   * difference between `max` and `min`.
   */
  def bigDecimal(min: BigDecimal, max: BigDecimal)(implicit trace: Trace): Gen[Any, BigDecimal] =
    if (min > max)
      Gen.fromZIO(ZIO.die(new IllegalArgumentException("invalid bounds")))
    else {
      val difference = max - min
      val decimals   = difference.scale max 0
      val bigInt     = (difference * BigDecimal(10).pow(decimals)).toBigInt
      Gen.bigInt(0, bigInt).map(bigInt => min + BigDecimal(bigInt) / BigDecimal(10).pow(decimals))
    }

  /**
   * A generator of [[java.math.BigDecimal]] inside the specified range: [start,
   * end]. The shrinker will shrink toward the lower end of the range
   * ("smallest").
   * @see
   *   See [[bigDecimal]] for implementation.
   */
  def bigDecimalJava(min: BigDecimal, max: BigDecimal)(implicit trace: Trace): Gen[Any, java.math.BigDecimal] =
    Gen.bigDecimal(min, max).map(_.underlying)

  /**
   * A generator of big integers inside the specified range: [start, end]. The