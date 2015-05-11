package climate.cmd

import com.typesafe.scalalogging.slf4j.LazyLogging

object Timer extends LazyLogging {
  // Time how long a block takes to execute.  From here:
  // http://stackoverflow.com/questions/9160001/how-to-profile-methods-in-scala
  def timedTask[R](msg: String)(block: => R): R = {
    val t0 = System.currentTimeMillis
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis
    println(msg + " in " + ((t1 - t0)) + " ms"    )
    logger.info(msg + " in " + ((t1 - t0)) + " ms")
    result
  }

  def timedTask[R](msg: String, reporter: String => Unit)(block: => R): R = {
    val t0 = System.currentTimeMillis
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis
    reporter(msg + " in " + ((t1 - t0)) + " ms")
    result
  }
}
