package com.asura.spark.sql.testhelper

import com.asura.spark.Entity.REFERENCEABLE_ATTRIBUTE_NAME
import com.asura.spark.{AsuraClient, Entity}
import com.asura.spark.client.MemoryClient

import scala.collection.JavaConverters._
import com.sun.jersey.core.util.MultivaluedMapImpl
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
 * @create 2021/11/9 9:46 上午
 * @author asura7969
 */
abstract class BaseResourceIT
  extends FunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private var client: AsuraClient = null
  private var uniquePostfix: Long = 0

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    client = new MemoryClient()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    uniquePostfix = System.currentTimeMillis()
  }

  protected def asuraClient: AsuraClient = {
    if (client == null) {
      throw new RuntimeException("asuraClient is null")
    }
    client
  }

  protected def getEntity(typeName: String, uniqueAttr: String): Entity = {
    require(asuraClient != null)

    asuraClient.getEntityByAttribute(typeName, uniqueAttr)
  }

  protected def it(desc: String)(testFn: => Unit): Unit = {
    test(desc) {
      assume(
        sys.env.get("ATLAS_INTEGRATION_TEST").contains("true"),
        "integration test can be run only when env ATLAS_INTEGRATION_TEST is set and local Atlas" +
          " is running")
      testFn
    }
  }

  protected def uniqueName(name: String): String = {
    s"${name}_$uniquePostfix"
  }
}
