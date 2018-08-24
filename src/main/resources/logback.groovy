appenders = ["STDOUT"]

appender("STDOUT", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
    }
}

gremlinLogs = System.getenv("GREMLIN_LOG_FOLDER")
if (gremlinLogs != null) {
    appender("ROLLING", RollingFileAppender) {
        encoder(PatternLayoutEncoder) {
            Pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
        }
        rollingPolicy(TimeBasedRollingPolicy) {
            FileNamePattern = "${gremlinLogs}/gremlin-cer-%d{yyyy-MM-dd}.zip"
        }
    }
    appenders.add("ROLLING")
}

root(ERROR, appenders)
logger("org.credentialengine.cer.gremlin", INFO, appenders, false)
