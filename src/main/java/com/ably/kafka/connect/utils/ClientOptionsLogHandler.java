package com.ably.kafka.connect.utils;

import io.ably.lib.util.Log;
import org.slf4j.Logger;


public class ClientOptionsLogHandler implements Log.LogHandler {
    private static final String[] severities = new String[]{"", "", "VERBOSE", "DEBUG", "INFO", "WARN", "ERROR", "ASSERT"};

    private final Logger logger;

    public ClientOptionsLogHandler(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void println(int severity, String tag, String msg, Throwable tr) {
        if (severity < 0 || severity >= severities.length) {
            severity = 3;
        }
        switch (severities[severity]) {
            case "VERBOSE":
                logger.trace(msg, tr);
                break;
            case "DEBUG":
                logger.debug(msg, tr);
                break;
            case "INFO":
                logger.info(msg, tr);
                break;
            case "WARN":
                logger.warn(msg, tr);
                break;
            case "ERROR":
                logger.error(msg, tr);
                break;
            case "default":
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        String.format(
                            "severity: %d, tag: %s, msg: %s, err",
                            severity, tag, msg, (tr != null) ? tr.getMessage() : "null"
                        )
                    );
                }
        }
    }
}
