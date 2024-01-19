package com.kafka.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogConfig {

    private Logger logger;

    public LogConfig(String name) {
        this.logger = LoggerFactory.getLogger(name);
    }

    public Logger getLogger() {
        return logger;
    }

    public static Logger name(String name) {
        return LoggerFactory.getLogger(name);
    }

}
