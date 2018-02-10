package io.kensu.hff
/** NOT USED **/

trait Direction
object UP extends Direction 
object DOWN extends Direction

case class TTick(time: Long, bid: Double, ask: Double) {
  def price() = (bid + ask) / 2 
}

case class DC(scale: Double, 
              mode: Direction, 
              xExt: Double, 
              x: Double, 
              t: Long) {
  def update(tick: TTick): DC = {
    val xi = tick.price
    val ti = tick.time
    mode match {
      case DOWN if (xi > xExt) => DC(scale, mode, xi, xi, ti)
      case DOWN if 100*(xExt-xi)/xExt >= scale  => DC(scale, UP, xi, xi, ti)
      case UP   if (xi < xExt) => DC(scale, mode, xi, xi, ti)
      case UP   if 100*(xi-xExt)/xExt >= scale  => DC(scale, DOWN, xi, xi, ti)
      case _ => DC(scale, mode, xExt, xi, ti)
    }
  }
}

object DC {
  def apply(scale: Double, tick: TTick): DC = 
    DC(scale, UP, tick.price, tick.price, tick.time)
}