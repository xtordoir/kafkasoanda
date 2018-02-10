package io.kensu.hff

import java.util.Date

case class Liquidity(price: Double, liquidity: Long)

case class PriceMsg(time: Date,
                    bid: Double,
                    ask: Double
                    )
