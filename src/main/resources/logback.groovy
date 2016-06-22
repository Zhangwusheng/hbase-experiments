import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.ConsoleAppender
import static ch.qos.logback.classic.Level.DEBUG
import static ch.qos.logback.classic.Level.ERROR
import static ch.qos.logback.classic.Level.WARN

appender("A1", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%d{yyyy/MM/dd-HH:mm:ss} %-5level [%thread] %class:%line >> %msg%n"
    }
}
logger("org.apache.hadoop", ERROR, ["A1"], false)
logger("org.apache.zookeeper", WARN, ["A1"], false)
logger("com.mogujie.mst", INFO, ["A1"], false)

root(ERROR, ["A1"])
