package io.getquill.context.zio

import java.sql.SQLException

import io.getquill.context.ContextEffect
import zio.{ Task, UIO, ZIO }
import zio.internal.Executor

object Runner {
  def default = new Runner {}
  def using(executor: Executor) = new Runner {
    override def schedule[T](t: Task[T]): Task[T] = t.lock(executor)
    override def boundary[T](t: Task[T]): Task[T] = Task.yieldNow *> t.lock(executor)
  }
}

trait Runner extends ContextEffect[Task] {
  override def wrap[T](t: => T): Task[T] = Task(t)
  override def push[A, B](result: Task[A])(f: A => B): Task[B] = result.map(f)
  override def seq[A](list: List[Task[A]]): Task[List[A]] = Task.collectAll(list)
  def schedule[T](t: Task[T]): Task[T] = t
  def boundary[T](t: Task[T]): Task[T] = Task.yieldNow *> t

  def catchAll[T](task: Task[T]): ZIO[Any, Nothing, Any] = task.catchAll {
    case _: SQLException              => Task.unit // TODO Log something. Can't have anything in the error channel... still needed
    case _: IndexOutOfBoundsException => Task.unit
    case e                            => Task.die(e): ZIO[Any, Nothing, Unit]
  }

  /**
   * Use this method whenever a ResultSet is being wrapped. This has a distinct
   * method because the client may prefer to fail silently on a ResultSet close
   * as opposed to failing the surrounding task.
   */
  def wrapClose(t: => Unit): UIO[Unit] = catchAll(Task(t)).unit
}
