package io.getquill.quotation

import scala.reflect.macros.whitebox.Context
import io.getquill.quat.Quat

trait QuatLiftable {
  val mctx: Context
  val serializeQuats: Boolean

  import mctx.universe.{ Ident => _, Constant => _, Function => _, If => _, Block => _, _ }

  // Liftables are invariant so want to have a separate liftable for Quat.Product since it is used directly inside Entity
  // which should be lifted/unlifted without the need for casting.
  implicit val quatProductLiftable: Liftable[Quat.Product] = Liftable[Quat.Product] {
    // If we are in the JVM, use Kryo to serialize our Quat due to JVM 64KB method limit that we will run into of the Quat Constructor
    // if plainly lifted into the method created by our macro (i.e. the 'ast' method).
    case quat: Quat.Product if (serializeQuats)                                  => q"io.getquill.quat.Quat.Product.fromSerializedJVM(${quat.serializeJVM})"
    case Quat.Product.WithRenamesCompact(fields, values, renamesFrom, renamesTo) => q"io.getquill.quat.Quat.Product.WithRenamesCompact.apply(..$fields)(..$values)(..$renamesFrom)(..$renamesTo)"
  }

  implicit val quatLiftable: Liftable[Quat] = Liftable[Quat] {
    case quat: Quat.Product if (serializeQuats) => q"io.getquill.quat.Quat.fromSerializedJVM(${quat.serializeJVM})"
    case Quat.Product.WithRenamesCompact(fields, values, renamesFrom, renamesTo) => q"io.getquill.quat.Quat.Product.WithRenamesCompact.apply(..$fields)(..$values)(..$renamesFrom)(..$renamesTo)"
    case Quat.Value => q"io.getquill.quat.Quat.Value"
    case Quat.Null => q"io.getquill.quat.Quat.Null"
    case Quat.Generic => q"io.getquill.quat.Quat.Generic"
    case Quat.BooleanValue => q"io.getquill.quat.Quat.BooleanValue"
    case Quat.BooleanExpression => q"io.getquill.quat.Quat.BooleanExpression"
  }
}