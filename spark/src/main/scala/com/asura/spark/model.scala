package com.asura.spark

import com.asura.spark.Entity.attributes

import scala.collection.immutable.HashMap

/**
 * @author asura7969
 * @create 2021-10-31-13:09
 */
class Entity(typeName: String) {
  private val relationshipAttributes = new HashMap[String, Entity]()

  def setAttribute(key: String, value: Any): Unit = {
    attributes + (key, value)
  }

  def setAttributes(options:Map[String, Any]): Unit = {
    options.foreach(kv => setAttribute(kv._1, kv._2))
  }

  def setRelationshipAttribute(key: String, value: Entity): Unit = {
    relationshipAttributes + (key, value)
  }

  def getAttribute(key: String): Any = attributes(key)
}

object Entity {
  val attributes = new HashMap[String, Any]()
}
