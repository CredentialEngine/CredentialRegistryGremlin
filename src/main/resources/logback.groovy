appenders = ["STDOUT"]

appender("STDOUT", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
    }
}

gremlinHome = System.getenv("GREMLIN_HOME")
if (gremlinHome != null) {
    appender("ROLLING", RollingFileAppender) {
        encoder(PatternLayoutEncoder) {
            Pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
        }
        rollingPolicy(TimeBasedRollingPolicy) {
            FileNamePattern = "${gremlinHome}/log/gremlin-cer-%d{yyyy-MM-dd}.zip"
        }
    }
    appenders.add("ROLLING")
}

root(ERROR, appenders)
logger("org.credentialengine.cer.gremlin", INFO, appenders, false)
