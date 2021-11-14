package com.asura.spark.client

import com.asura.spark.Entity.REFERENCEABLE_ATTRIBUTE_NAME
import com.asura.spark.{AsuraClient, Entity}

import scala.collection.JavaConverters._
import scala.collection.mutable
/**
 * @Date 2021/11/9 9:59 上午
 * @author asura7969
 */
class MemoryClient extends AsuraClient {

  private val attributes = new mutable.HashMap[PersistenceEntity, Entity]

  override def getEntityByAttribute(typeName: String, uniqueAttr: String): Entity = {
    for (elem <- attributes) {
      val perEntity = elem._1
      if (perEntity.typeName.equals(typeName)
        && perEntity.containsAttribute(REFERENCEABLE_ATTRIBUTE_NAME, uniqueAttr)) {
        return elem._2
      } else {
        elem._2.findRelationship((e) => e.getTypeName.equals(typeName)
          && e.containsAttribute(REFERENCEABLE_ATTRIBUTE_NAME, uniqueAttr)) match {
          case Some(entity) => return entity
          case None => return null
        }
      }
    }
    null
  }

  override def saveEntity(entity: Entity): Unit = {
    val persistenceEntity = new PersistenceEntity(entity.getTypeName, Map(REFERENCEABLE_ATTRIBUTE_NAME ->
      entity.getAttribute(REFERENCEABLE_ATTRIBUTE_NAME)))

    attributes.put(persistenceEntity, entity)
    logInfo(s"保存Entity, typeName: ${persistenceEntity.typeName}, uniqueAttr: ${entity.getAttribute(REFERENCEABLE_ATTRIBUTE_NAME)}")
  }

  override def updateEntity(typeName: String, uniqueAttr: String, replace: Entity): Unit ={
    Option(getEntityByAttribute(typeName, uniqueAttr)) match {
      case Some(entity) =>
        replace.getAllAttributes.foreach(t => entity.setAttribute(t._1, t._2))
      case None =>
        logWarn(s"未匹配到修改项,typeName: $typeName, uniqueAttr: $uniqueAttr")
    }
  }


  override def deleteEntity(typeName: String, uniqueAttr: String): Unit = {
    val persistenceEntity = new PersistenceEntity(typeName, Map(REFERENCEABLE_ATTRIBUTE_NAME -> uniqueAttr))
    attributes.remove(persistenceEntity)
  }

}


class PersistenceEntity(val typeName: String, attribute: Map[String, Any]) {

  def containsAttribute(key: String, value: Any): Boolean = {
    attribute.get(key).exists(_.equals(value))
  }
}
