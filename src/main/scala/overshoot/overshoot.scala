package io.kensu.hff

/**
Tick holds price information: bid and ask
*/
case class Tick(bid: Double, ask: Double) {
  def price() = (bid + ask) / 2 
}

/**
Overshoot for a given Scale (in %)
Direction: (+1 = up, -1 = down)
prevExt: Extremum before Directional change
ext: Current extremum in this Direction
*/
case class Overshoot(scale: Double, 
                     direction: Int, 
                     prevExt: Double, 
                     ext: Double,
                     price: Double) {
    /**
     update Overshoot with a new tick
    */
  def update(price: Tick, sc: Double): Overshoot = {
        // we are doing regular update as the overshoot was already initialized
    if (prevExt > 0) update(price)
    // we are initializing the Overshoot setting the scale and ext
    else Overshoot(sc, direction, price.price * (1.0-scale/100), price.price, price.price)
  }
    
  def update(price: Tick): Overshoot = {
    val x = price.price
  val os = 100*(x - prevExt)/prevExt/scale
  val eDist = 100*(x - ext)/ext/scale
    if (prevExt < 0) {
      Overshoot(scale, direction, x * (1.0-scale/100), x, x)
    }
  // if reversal...
  else if (os*eDist < 0 && math.abs(eDist) > 1.0) {
    Overshoot(scale, -direction, ext, x, x)
  } 
    // if new extremum
    else if ( (x - ext) * direction >= 0.00000001) {
    //println((x - ext) * direction)
        Overshoot(scale, direction, prevExt, x, x)
  } else {
        Overshoot(scale, direction, prevExt, ext, x)
  }
    }
}

object Overshoot {
  def apply(scale: Double): Overshoot = Overshoot(scale, 1, -1.0, -1.0, -1.0)
}

case class DChange(scale: Double, 
                   direction: Int, 
                   prevExt: Double) {
  def update(os: Overshoot, sc: Double): DChange = {
    if (direction == 0) DChange(sc, os.direction, os.prevExt) else update(os)
  }
  def update(os: Overshoot): DChange = if (os.direction != direction) DChange(os) else DChange(scale, direction, prevExt)
}
object DChange {
  def apply(os: Overshoot): DChange = DChange(os.scale, os.direction, os.prevExt)
}
