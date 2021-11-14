
package com.asura.spark

import com.asura.spark.util.Logging

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import com.google.common.annotations.VisibleForTesting

abstract class AbstractEventProcessor[T: ClassTag] extends Logging {
  def conf: Conf

  private val capacity = conf.get(Conf.BLOCKING_QUEUE_CAPACITY).toInt

  private[asura] val eventQueue = new LinkedBlockingQueue[T](capacity)

  private val timeout = conf.get(Conf.BLOCKING_QUEUE_PUT_TIMEOUT).toInt

  private val eventProcessThread = new Thread {
    override def run(): Unit = {
      eventProcess()
    }
  }

  def pushEvent(event: T): Unit = {
    event match {
      case e: T =>
        if (!eventQueue.offer(e, timeout, TimeUnit.MILLISECONDS)) {
          logError(s"Fail to put event $e into queue within time limit $timeout, will throw it")
        }
      case _ => // Ignore other events
    }
  }

  def startThread(): Unit = {
    eventProcessThread.setName(this.getClass.getSimpleName + "-thread")
    eventProcessThread.setDaemon(true)

    val ctxClassLoader = Thread.currentThread().getContextClassLoader
    if (ctxClassLoader != null && getClass.getClassLoader != ctxClassLoader) {
      eventProcessThread.setContextClassLoader(ctxClassLoader)
    }

    eventProcessThread.start()
  }

  protected def process(e: T): Unit

  @VisibleForTesting
  private[asura] def eventProcess(): Unit = {
    var stopped = false
    while (!stopped) {
      try {
        Option(eventQueue.take()).foreach { e =>
          process(e)
        }
//        Option(eventQueue.poll(3000, TimeUnit.MILLISECONDS)).foreach { e =>
//          process(e)
//        }
      } catch {
        case _: InterruptedException =>
          logDebug("Thread is interrupted")
          stopped = true

        case NonFatal(f) =>
          logWarn(s"Caught exception during parsing event", f)
      }
    }
  }
}
