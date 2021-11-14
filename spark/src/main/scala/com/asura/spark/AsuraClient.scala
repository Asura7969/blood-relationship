package com.asura.spark

import com.asura.spark.util.Logging

/**
 * @author asura7969
 * @create 2021-10-25-21:43
 */
trait AsuraClient extends Logging {

  def getEntityByAttribute(typeName: String, uniqueAttr: String): Entity

  def saveEntity(entity: Entity): Unit

  def updateEntity(typeName: String, uniqueAttr: String, replace: Entity): Unit

  def deleteEntity(typeName: String, uniqueAttr: String): Unit
}
