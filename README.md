This Application consumes a topic containing Oanda Stream Pricing messages in a `pricing` topic.
Overshoots are computed and produced in and `overshoots` topic. Then directional changes are published into `dchange`.

Configuration is done using an application.conf file:

```
## ID for application, defining the consumer group
kafka.application.id = "overshoots"
kafka.auto.offset.reset="earliest"

## Interval for commits
kafka.commit.interval.ms="250"

## Kafka brokers
kafka.bootstrap.servers="localhost:9092"
```
