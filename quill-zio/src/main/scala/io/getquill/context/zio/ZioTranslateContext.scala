package io.getquill.context.zio

import io.getquill.NamingStrategy
import io.getquill.context.{ Context, ContextEffect, TranslateContextBase }
import io.getquill.idiom.Idiom
import zio.Task
import zio.stream.Stream

trait ZioTranslateContext extends TranslateContextBase {
  this: Context[_ <: Idiom, _ <: NamingStrategy] =>

  override type TranslateResult[T] = Task[T]

  override private[getquill] val translateEffect: ContextEffect[Task] = new ContextEffect[Task] {
    override def wrap[T](t: => T): Task[T] = Task(t)
    override def push[A, B](result: Task[A])(f: A => B): Task[B] = result.map(f)
    override def seq[A, B](list: List[Task[A]]): Task[List[A]] = Task.collectAll(list)
  }
}
