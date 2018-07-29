appender("STDOUT", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
    }
}
root(ERROR, ["STDOUT"])
logger("org.credentialengine.cer.gremlin", DEBUG, ["STDOUT"], false)
