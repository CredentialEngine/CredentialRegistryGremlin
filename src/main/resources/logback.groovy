appenders = ["STDOUT"]

appender("STDOUT", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
    }
}

if (context.getProperty("GREMLIN_CER_DIR") != null) {
    appender("ROLLING", RollingFileAppender) {
        encoder(PatternLayoutEncoder) {
            Pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
        }
        rollingPolicy(TimeBasedRollingPolicy) {
            FileNamePattern = "${GREMLIN_CER_DIR}/log/gremlin-cer-%d{yyyy-MM-dd}.zip"
        }
    }
    appenders.add("ROLLING")
}

root(ERROR, appenders)
logger("org.credentialengine.cer.gremlin", INFO, appenders, false)
