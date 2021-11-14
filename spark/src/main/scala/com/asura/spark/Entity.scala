package com.asura.spark

import com.asura.spark.Entity.REFERENCEABLE_ATTRIBUTE_NAME

import scala.collection.mutable

/**
 * @author asura7969
 * @create 2021-10-31-13:09
 */
class Entity(typeName: String) {
  private val relationshipAttributes = new mutable.HashMap[String, Entity]
  private val attributes = new mutable.HashMap[String, Any]

  def setAttribute(key: String, value: Any): Unit = {
    attributes.put(key, value)
  }

  def setAttributes(options:Map[String, Any]): Unit = {
    options.foreach(kv => setAttribute(kv._1, kv._2))
  }

  def setRelationshipAttribute(key: String, value: Entity): Unit = {
    relationshipAttributes.put(key, value)
  }

  def findRelationship(p: Entity => Boolean): Option[Entity] = {
    relationshipAttributes.values.find(p)
  }

  def containsAttribute(key: String, value: Any): Boolean = {
    attributes.get(key).exists(_.equals(value))
  }

  def getAttribute(key: String): Any = attributes(key)

  def getAllAttributes: mutable.HashMap[String, Any] = attributes

  def qualifiedName: String = {
    attributes.getOrElse(REFERENCEABLE_ATTRIBUTE_NAME,"")
      .asInstanceOf[String]
  }

  def getTypeName: String = typeName
}

object Entity {

  val REFERENCEABLE_ATTRIBUTE_NAME = "qualifiedName";
}
