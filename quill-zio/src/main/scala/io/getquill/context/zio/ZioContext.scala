package io.getquill.context.zio

import io.getquill.NamingStrategy
import io.getquill.context.{ Context, StreamingContext }
import zio.Task
import zio.stream.Stream

trait ZioContext[Idiom <: io.getquill.idiom.Idiom, Naming <: NamingStrategy] extends Context[Idiom, Naming]
  with StreamingContext[Idiom, Naming] {

  override type StreamResult[T] = Stream[Throwable, T]
  override type Result[T] = Task[T]
  override type RunQueryResult[T] = List[T]
  override type RunQuerySingleResult[T] = T

  // Need explicit return-type annotations due to scala/bug#8356. Otherwise macro system will not understand Result[Long]=Task[Long] etc...
  def executeQuery[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): Task[List[T]]
  def executeQuerySingle[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): Task[T]
}
