package com.asura.spark

import com.asura.spark.Entity.{REFERENCEABLE_ATTRIBUTE_NAME, attributes}

import scala.collection.immutable.HashMap

/**
 * @author asura7969
 * @create 2021-10-31-13:09
 */
class Entity(typeName: String) {
  private val relationshipAttributes = new HashMap[String, Entity]()

  def setAttribute(key: String, value: Any): Unit = {
    relationshipAttributes.+((key, value))
  }

  def setAttributes(options:Map[String, Any]): Unit = {
    options.foreach(kv => setAttribute(kv._1, kv._2))
  }

  def setRelationshipAttribute(key: String, value: Entity): Unit = {
    relationshipAttributes + ((key, value))
  }

  def getAttribute(key: String): Any = attributes(key)

  def qualifiedName: String = {
    attributes.getOrElse(REFERENCEABLE_ATTRIBUTE_NAME,"")
      .asInstanceOf[String]
  }

  def getTypeName: String = typeName
}

object Entity {
  val attributes = new HashMap[String, Any]()
  val REFERENCEABLE_ATTRIBUTE_NAME = "qualifiedName";
}
