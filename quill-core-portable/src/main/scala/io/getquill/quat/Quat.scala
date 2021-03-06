package io.getquill.quat

import io.getquill.quotation.QuatException

import scala.collection.mutable

object LinkedHashMapOps {
  implicit class LinkedHashMapExt[K, V](m1: mutable.LinkedHashMap[K, V]) {
    def zipWith[R](m2: mutable.LinkedHashMap[K, V])(f: PartialFunction[(K, V, Option[V]), R]) =
      LinkedHashMapOps.zipWith(m1, m2, f)

    def outerZipWith[R](m2: mutable.LinkedHashMap[K, V])(f: PartialFunction[(K, Option[V], Option[V]), R]) =
      LinkedHashMapOps.outerZipWith(m1, m2, f)
  }

  def zipWith[K, V, R](m1: mutable.LinkedHashMap[K, V], m2: mutable.LinkedHashMap[K, V], f: PartialFunction[(K, V, Option[V]), R]) = {
    m1.toList.map(r => (r._1, r._2, m2.get(r._1))).collect(f)
  }

  def outerZipWith[K, V, R](m1: mutable.LinkedHashMap[K, V], m2: mutable.LinkedHashMap[K, V], f: PartialFunction[(K, Option[V], Option[V]), R]) = {
    mutable.LinkedHashSet((m1.keySet.toList ++ m2.keySet.toList): _*).map(k => (k, m1.get(k), m2.get(k))).collect(f)
  }
}

/**
 * This represents a simplified Sql-Type. Since it applies to all dialects, it is called
 * Quill-Application-Type hence Quat. Quats represent the types of the Quill AST allowing us
 * to know what fields exist on an Ident and more concretely what can be done with it.
 * Currently they are:
 * <ul>
 *   <li> Quat.Product - Something that contains a list of fields e.g. case class `Person(name:String,age:Int)` would be
 *   `Product("name"->Quat.Value, "age"->Quat.Value)`
 *   <li> Quat.Value - A value level Quat e.g. Int, Long, String etc... More specific Quat.Value types are planned int the future.
 *   <li> Quat.Generic - A Quat representing a type whose value is not known yet.
 *   <li> Quat.Null - A Quat representing a null value.
 * </ul>
 *
 * Quats are generally treated as through they are immutable although there are dynamic/mutable variables in some places
 * (e.g. Product uses LinkedHashSet and var) due to various other issues (e.g. performance, serialization). It is assumed that
 * all operations Quats have referential transparency.
 */
sealed trait Quat {
  def applyRenames: Quat = this
  def withRenames(renames: mutable.LinkedHashMap[String, String]): Quat
  def withRenames(renames: List[(String, String)]): Quat =
    withRenames(mutable.LinkedHashMap(renames: _*))

  def serializeJVM = KryoQuatSerializer.serialize(this)
  def serializeJS = BooQuatSerializer.serialize(this)

  /** Recursively count the fields of the Quat */
  def countFields: Int =
    this match {
      case p: Quat.Product => p.fields.map(kv => kv._2.countFields).sum + 1
      case _               => 1
    }

  def renames: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap()

  /** Either convert to a Product or make the Quat into an error if it is anything else. */
  def probit =
    this match {
      case p: Quat.Product => p
      case other           => QuatException(s"Was expecting SQL-level type must be a Product but found `${other}`")
    }

  def leastUpperType(other: Quat): Option[Quat] = {
    (this, other) match {
      case (Quat.Generic, other) => Some(other)
      case (Quat.Null, other) => Some(other)
      case (other, Quat.Generic) => Some(other)
      case (other, Quat.Null) => Some(other)
      case (Quat.Value, Quat.Value) => Some(Quat.Value)
      case (Quat.BooleanExpression, Quat.BooleanExpression) => Some(Quat.BooleanExpression)
      case (Quat.BooleanValue, Quat.BooleanValue) => Some(Quat.BooleanValue)
      case (Quat.BooleanValue, Quat.BooleanExpression) => Some(Quat.BooleanValue)
      case (Quat.BooleanExpression, Quat.BooleanValue) => Some(Quat.BooleanValue)
      case (Quat.Value, Quat.BooleanValue) => Some(Quat.Value)
      case (Quat.Value, Quat.BooleanExpression) => Some(Quat.Value)
      case (Quat.BooleanValue, Quat.Value) => Some(Quat.Value)
      case (Quat.BooleanExpression, Quat.Value) => Some(Quat.Value)
      case (me: Quat.Product, other: Quat.Product) => me.leastUpperTypeProduct(other)
      case (_, _) => None
    }
  }

  override def toString: String = shortString

  def shortString: String = this match {
    case Quat.Product(fields) => s"CC(${
      fields.map {
        case (k, v) => k + (v match {
          case other => ":" + other.shortString
        })
      }.mkString(",")
    })${
      (if (this.renames.isEmpty)
        ""
      else
        s"[${this.renames.map { case (k, v) => k + "->" + v }.mkString(",")}]")
    }"
    case Quat.Generic           => "<G>"
    case Quat.Value             => "V"
    case Quat.Null              => "N"
    case Quat.BooleanValue      => "BV"
    case Quat.BooleanExpression => "BE"
  }

  /** What was the value of a given property before it was renamed (i.e. looks up the value of the Renames hash) */
  def beforeRenamed(path: String): Option[String] = (this, path) match {
    case (cc: Quat.Product, fieldName) =>
      // NOTE This is a linear lookup. To improve efficiency store a map going back from rename to the initial property,
      // if we did that however, we would need to make sure to warn a user of two things are renamed to the same property however,
      // that kind of warning should probably exist already
      renames.find(_._2 == fieldName).headOption.map(_._2)
    case (other, fieldName) =>
      QuatException(s"The post-rename field '${fieldName}' does not exist in an SQL-level type ${other}")
  }

  def lookup(path: String): Quat = (this, path) match {
    case (cc @ Quat.Product(fields), fieldName) =>
      fields.get(fieldName).getOrElse(QuatException(s"The field ${fieldName} does not exist in the SQL-level ${cc}"))
    case (other, fieldName) =>
      QuatException(s"The field '${fieldName}' does not exist in an SQL-level type ${other}")
  }
  def lookup(list: List[String]): Quat =
    list match {
      case head :: tail => this.lookup(head).lookup(tail)
      case Nil          => this
    }
}

object Quat {
  import LinkedHashMapOps._

  def fromSerializedJVM(serial: String): Quat = KryoQuatSerializer.deserialize(serial)
  def fromSerializedJS(serial: String): Quat = BooQuatSerializer.deserialize(serial)

  case class Product(fields: mutable.LinkedHashMap[String, Quat]) extends Quat {
    def this(list: Iterator[(String, Quat)]) = this((mutable.LinkedHashMap[String, Quat]() ++ list): mutable.LinkedHashMap[String, Quat])

    // Strictly internal, use WithRenames to construct an instance of a Product with Renames
    private def this(fields: mutable.LinkedHashMap[String, Quat], newRenames: mutable.LinkedHashMap[String, String]) = {
      this(fields)
      _renames = newRenames
    }

    // Defining as var but should be effectively static! I.e. changes only possible with WithRename. Need
    // to make this var because doing "new Product { override def renames = newRenames }" breaks BooPickle serialization
    var _renames: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap()
    override def renames: mutable.LinkedHashMap[String, String] = _renames

    def withRenamesFrom(other: Quat): Quat = {
      other match {
        case otherProduct: Quat.Product =>
          val newFields =
            fields.map {
              case (key, value) =>
                otherProduct.fields.find(_._1 == key).map { case (ok, ov) => (key, ov, value) }.toRight((key, value))
            }.map {
              // If the other Quat.Product does not have this field, don't rename it, just return it as is
              case Left((key, value))                       => (key, value)
              // If the other Quat.Product has this field and the value of that field is also a product, recurse into it
              case Right((key, from: Product, to: Product)) => (key, to.withRenamesFrom(from))
              // If the value of the other field is not a product, just return the original field/value
              case Right((key, _, to))                      => (key, to)
            }
          // Pass in the local renames from the other quat product
          Quat.Product(newFields).withRenames(otherProduct.renames)

        case _ => this
      }
    }

    def leastUpperTypeProduct(other: Quat.Product): Option[Quat.Product] = {
      val newFieldsIter =
        fields.zipWith(other.fields) {
          case (key, thisQuat, Some(otherQuat)) => (key, thisQuat.leastUpperType(otherQuat))
        }.collect {
          case (key, Some(value)) => (key, value)
        }
      val newFields = mutable.LinkedHashMap(newFieldsIter.toList: _*)
      // Note, some extra renames from properties that don't exist could make it here.
      // Need to make sure to ignore extra ones when they are actually applied.
      Some(Quat.Product(newFields).withRenames(renames))
    }

    override def withRenames(renames: mutable.LinkedHashMap[String, String]): Quat.Product =
      Product.WithRenames(fields, renames)

    override def withRenames(renames: List[(String, String)]): Quat.Product =
      Product.WithRenames(fields, (mutable.LinkedHashMap[String, String]() ++ renames): mutable.LinkedHashMap[String, String])

    /**
     * Rename the properties based on the renames list. Keep this list
     * around since it is used in sql sub-query expansion to determine whether
     * the property is fixed or not (i.e. whether the column naming strategy should
     * be applied to it).
     */
    override def applyRenames: Quat.Product = {
      val newFields = fields.map {
        case (f, q) =>
          // Rename properties of this quat and rename it's children recursively
          val newKey = renames.get(f).getOrElse(f)
          val newValue = q.applyRenames
          (newKey, newValue)
      }
      Product.WithRenames(newFields, renames)
    }
  }
  def LeafProduct(list: String*) = Quat.Product(list.map(e => (e, Quat.Value)))
  def LeafTuple(numElems: Int) = Quat.Tuple((1 to numElems).map(_ => Quat.Value))

  object Product {
    def fromSerializedJVM(serial: String): Quat.Product = KryoQuatSerializer.deserialize(serial).probit
    def fromSerializedJS(serial: String): Quat.Product = BooQuatSerializer.deserialize(serial).probit

    def empty = new Quat.Product(mutable.LinkedHashMap[String, Quat]())
    def apply(fields: (String, Quat)*): Quat.Product = apply(fields.iterator)
    def apply(fields: Iterable[(String, Quat)]): Quat.Product = new Quat.Product(fields.iterator)
    def apply(fields: Iterator[(String, Quat)]): Quat.Product = new Quat.Product(fields)

    /**
     * Add staged renames to the Quat. Note that renames should
     * explicit NOT be counted as part of the Type indicated by the Quat
     * since it is typical to beta reduce a Quat without renames to a Quat with them
     * (see `PropagateRenames` for more detail)
     */
    object WithRenames {
      def apply(fields: mutable.LinkedHashMap[String, Quat], theRenames: mutable.LinkedHashMap[String, String]) =
        new Product(fields, theRenames)

      def iterated(list: Iterator[(String, Quat)], renames: Iterator[(String, String)]) =
        WithRenames.apply(
          (mutable.LinkedHashMap[String, Quat]() ++ list): mutable.LinkedHashMap[String, Quat],
          (mutable.LinkedHashMap[String, String]() ++ renames): mutable.LinkedHashMap[String, String]
        )

      def unapply(p: Quat.Product) =
        Some((p.fields, p.renames))
    }

    object WithRenamesCompact {
      def apply(fields: String*)(values: Quat*)(renamesFrom: String*)(renamesTo: String*) = {
        if (fields.length != values.length)
          throw new IllegalArgumentException(
            s"Property Re-creation failed because fields length ${fields.length} was not same as values length ${values.length}." +
              s"\nFields: [${fields.mkString(", ")}]" + s"\nValues: [${values.mkString(", ")}]"
          )
        if (renamesFrom.length != renamesTo.length)
          throw new IllegalArgumentException(
            s"Property Re-creation failed because rename keys length ${renamesFrom.length} was not same as rename values length ${renamesTo.length}." +
              s"\nKeys: [${renamesFrom.mkString(", ")}]" + s"\nValues: [${renamesTo.mkString(", ")}]"
          )
        Product.WithRenames.iterated(fields.zip(values).iterator, renamesFrom.zip(renamesTo).iterator)
      }

      def unapply(p: Quat.Product) = {
        val (fields, values) = p.fields.unzip
        val (renamesFrom, renamesTo) = p.renames.unzip
        Some((fields, values, renamesFrom, renamesTo))
      }
    }
  }
  object Tuple {
    def apply(fields: Quat*): Quat.Product = apply(fields)
    def apply(fields: Iterable[Quat]): Quat.Product = new Quat.Product(fields.zipWithIndex.map { case (f, i) => (s"_${i + 1}", f) }.iterator)
  }
  case object Null extends Quat {
    override def withRenames(renames: mutable.LinkedHashMap[String, String]) = this
  }
  case object Generic extends Quat {
    override def withRenames(renames: mutable.LinkedHashMap[String, String]) = this
  }

  case object Value extends Quat with NoRenames
  sealed trait Boolean extends Quat
  case object BooleanValue extends Boolean with NoRenames
  case object BooleanExpression extends Boolean with NoRenames

  protected trait NoRenames {
    this: Quat =>

    /** Should not be able to rename properties on a value node, turns into a error of the array is not null */
    override def withRenames(renames: mutable.LinkedHashMap[String, String]): Quat = {
      if (renames.isEmpty)
        this
      else
        QuatException(s"Renames $renames cannot be applied to a value SQL-level type")
    }
  }
}
