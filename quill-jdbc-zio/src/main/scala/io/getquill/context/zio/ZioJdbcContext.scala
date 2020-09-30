package io.getquill.context.zio

import java.io.Closeable
import java.sql.{Array => _, _}

import io.getquill.{NamingStrategy, ReturnAction}
import io.getquill.context.StreamingContext
import io.getquill.context.jdbc.JdbcContextBase
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.util.ContextLogger
import javax.sql.DataSource
import zio.Exit.{Failure, Success}
import zio.{Chunk, FiberRef, Task, UIO, ZIO}
import zio.stream.{Stream, ZStream}

import scala.collection.mutable
import scala.util.Try

/**
 * Quill context that wraps all JDBC calls in `monix.eval.Task`.
 *
 */
abstract class ZioJdbcContext[Dialect <: SqlIdiom, Naming <: NamingStrategy](
  dataSource: DataSource with Closeable, runner: Runner
) extends ZioContext[Dialect, Naming]
  with JdbcContextBase[Dialect, Naming]
  with StreamingContext[Dialect, Naming]
  with ZioTranslateContext {

  override private[getquill] val logger = ContextLogger(classOf[ZioJdbcContext[_, _]])

  override def prepareSingle(sql: String, prepare: Prepare): Connection => Task[PreparedStatement] = super.prepareSingle(sql, prepare)

  override type PrepareRow = PreparedStatement
  override type ResultRow = ResultSet
  override type RunActionResult = Long
  override type RunActionReturningResult[T] = T
  override type RunBatchActionResult = List[Long]
  override type RunBatchActionReturningResult[T] = List[T]

  // Need explicit return-type annotations due to scala/bug#8356. Otherwise macro system will not understand Result[Long]=Task[Long] etc...
  override def executeAction[T](sql: String, prepare: Prepare = identityPrepare): Task[Long] =
    super.executeAction(sql, prepare)
  override def executeQuery[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): Task[List[T]] =
    super.executeQuery(sql, prepare, extractor)
  override def executeQuerySingle[T](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): Task[T] =
    super.executeQuerySingle(sql, prepare, extractor)
  override def executeActionReturning[O](sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[O], returningBehavior: ReturnAction): Task[O] =
    super.executeActionReturning(sql, prepare, extractor, returningBehavior)
  override def executeBatchAction(groups: List[BatchGroup]): Task[List[Long]] =
    super.executeBatchAction(groups)
  override def executeBatchActionReturning[T](groups: List[BatchGroupReturning], extractor: Extractor[T]): Task[List[T]] =
    super.executeBatchActionReturning(groups, extractor)
  override def prepareQuery[T](sql: String, prepare: Prepare, extractor: Extractor[T] = identityExtractor): Connection => Task[PreparedStatement] =
    super.prepareQuery(sql, prepare, extractor)
  override def prepareAction(sql: String, prepare: Prepare): Connection => Task[PreparedStatement] =
    super.prepareAction(sql, prepare)
  override def prepareBatchAction(groups: List[BatchGroup]): Connection => Task[List[PreparedStatement]] =
    super.prepareBatchAction(groups)

  override protected val effect: Runner = runner
  import effect._

  private val currentConnection: UIO[FiberRef[Option[Connection]]] = FiberRef.make(None)

  override def close(): Unit = dataSource.close()

  override protected def withConnection[T](f: Connection => Task[T]): Task[T] =
    for {
      maybeConnectionRef <- currentConnection
      maybeConnection <- maybeConnectionRef.get
      result <- maybeConnection match {
        case Some(connection) => f(connection)
        case None =>
          schedule {
            wrap(dataSource.getConnection).bracket(conn => wrapClose(conn.close()))(f)
          }
      }
    } yield result

  protected def withConnectionObservable[T](f: Connection => Stream[Throwable, T]): Stream[Throwable, T] =
    for {
      maybeConnectionRef <- Stream.fromEffect(currentConnection)
      maybeConnection <- Stream.fromEffect(maybeConnectionRef.get)
      result <- maybeConnection match {
        case Some(connection) =>
          withAutocommitBracket(connection, f)
        case None =>
          Stream.bracket(
            Task(dataSource.getConnection)
          )(conn => wrapClose(conn.close())).flatMap(conn => // Note: Can use mapM instead
              withAutocommitBracket(conn, f))
      }
    } yield result

  /**
   * Need to store, set and restore the client's autocommit mode since some vendors (e.g. postgres)
   * don't like autocommit=true during streaming sessions. Using brackets to do that.
   */
  private[getquill] def withAutocommitBracket[T](conn: Connection, f: Connection => Stream[Throwable, T]): Stream[Throwable, T] = {
    Stream.bracket(Task(autocommitOff(conn)))(autoCommitBackOn)
      .flatMap({ case (conn, _) => f(conn) })
  }

  private[getquill] def withAutocommitBracket[T](conn: Connection, f: Connection => Task[T]): Task[T] = {
    Task(autocommitOff(conn))
      .bracket(autoCommitBackOn)({ case (conn, _) => f(conn) })
  }

  private[getquill] def withCloseBracket[T](conn: Connection, f: Connection => Task[T]): Task[T] = {
    Task(conn)
      .bracket(conn => wrapClose(conn.close()))(conn => f(conn))
  }

  private[getquill] def autocommitOff(conn: Connection): (Connection, Boolean) = {
    val ac = conn.getAutoCommit;
    conn.setAutoCommit(false);
    (conn, ac)
  }

  private[getquill] def autoCommitBackOn(state: (Connection, Boolean)) = {
    val (conn, wasAutocommit) = state
    wrapClose(conn.setAutoCommit(wasAutocommit))
  }

  def transaction[A](f: Task[A]): Task[A] = {
    val dbEffects = for {
      connectionRef <- currentConnection
      connection <- connectionRef.get
      result <- connection match {
        case Some(_) => f // Already in a transaction
        case None =>
          wrap(dataSource.getConnection).bracket { conn =>
            // TODO This returns UIO, is that correct?
            connectionRef.set(None)
          } { conn =>
            withCloseBracket(conn, conn => {
              withAutocommitBracket(conn, conn => {
                wrap(conn).flatMap { conn =>
                  connectionRef.set(Some(conn))
                  f.onInterrupt(Task.die(new IllegalStateException(
                    "The task was cancelled in the middle of a transaction."
                  ))).onExit {
                    case Success(_) =>
                      wrapClose(conn.commit())
                    case Failure(cause) =>
                      // TODO Are we really catching the result of the conn.rollback() Task's exception?
                      catchAll(Task(conn.rollback()) *> Task.halt(cause))
                  }
                }
              })
            })
          }
      }
    } yield result

    boundary {
      schedule(dbEffects)
    }
  }

  // Override with sync implementation so will actually be able to do it.
  override def probe(sql: String): Try[_] = Try {
    val c = dataSource.getConnection
    try {
      c.createStatement().execute(sql)
    } finally {
      c.close()
    }
  }

  /**
   * In order to allow a ResultSet to be consumed by an Observable, a ResultSet iterator must be created.
   * Since Quill provides a extractor for an individual ResultSet row, a single row can easily be cached
   * in memory. This allows for a straightforward implementation of a hasNext method.
   */
  class ResultSetIterator[T](rs: ResultSet, extractor: Extractor[T]) extends BufferedIterator[T] {

    private[this] var state = 0 // 0: no data, 1: cached, 2: finished
    private[this] var cached: T = null.asInstanceOf[T]

    protected[this] final def finished(): T = {
      state = 2
      null.asInstanceOf[T]
    }

    /** Return a new value or call finished() */
    protected def fetchNext(): T =
      if (rs.next()) extractor(rs)
      else finished()

    def head: T = {
      prefetchIfNeeded()
      if (state == 1) cached
      else throw new NoSuchElementException("head on empty iterator")
    }

    private def prefetchIfNeeded(): Unit = {
      if (state == 0) {
        cached = fetchNext()
        if (state == 0) state = 1
      }
    }

    def hasNext: Boolean = {
      prefetchIfNeeded()
      state == 1
    }

    def next(): T = {
      prefetchIfNeeded()
      if (state == 1) {
        state = 0
        cached
      } else throw new NoSuchElementException("next on empty iterator");
    }
  }

  /**
   * Override to enable specific vendor options needed for streaming
   */
  protected def prepareStatementForStreaming(sql: String, conn: Connection, fetchSize: Option[Int]) = {
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    fetchSize.foreach { size =>
      stmt.setFetchSize(size)
    }
    stmt
  }

  def streamQuery[T](fetchSize: Option[Int], sql: String, prepare: Prepare = identityPrepare, extractor: Extractor[T] = identityExtractor): Stream[Throwable, T] =
    withConnectionObservable { conn =>
      Stream.bracket({
        Task {
          val stmt = prepareStatementForStreaming(sql, conn, fetchSize)
          val (params, ps) = prepare(stmt)
          logger.logQuery(sql, params)
          ps.executeQuery()
        }
      })({ rs =>
        wrapClose(rs.close())
      }).flatMap { rs =>
        val iter = new ResultSetIterator(rs, extractor)
        fetchSize match {
          // TODO Assuming chunk size is fetch size. Not sure if this is optimal. Maybe introduce some switches to control this?
          case Some(size) =>
            chunkedFetch(iter, size)
          case None =>
            Stream.fromIterator(new ResultSetIterator(rs, extractor))
        }
      }
    }

  def chunkedFetch[T](iter: ResultSetIterator[T], fetchSize: Int) = {
    object StreamEnd extends Throwable
    ZStream.fromEffect(Task(iter) <*> ZIO.runtime[Any]).flatMap {
      case (it, rt) =>
        ZStream.repeatEffectChunkOption {
          Task {
            val hasNext: Boolean =
              try it.hasNext
              catch {
                case e: Throwable if !rt.platform.fatal(e) =>
                  throw e
              }
            if (hasNext) {
              try {
                val arr = it.take(fetchSize).toArray
                Chunk.fromArray(arr)
              } catch {
                case e: Throwable if !rt.platform.fatal(e) =>
                  throw e
              }
            } else throw StreamEnd
          }.mapError {
            case StreamEnd => None
            case e         => Some(e)
          }
        }
    }
  }

  override private[getquill] def prepareParams(statement: String, prepare: Prepare): Task[Seq[String]] = {
    withConnectionWrapped { conn =>
      prepare(conn.prepareStatement(statement))._1.reverse.map(prepareParam)
    }
  }
}
