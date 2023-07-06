package com.ably.kafka.connect.batch;

/**
 * An exception type to enable batch processing work to explicitly shut down the whole
 * sink task if misconfiguration or some other unresolvable issue is detected during processing.
 *
 */
public class FatalBatchProcessingException extends RuntimeException {
    public FatalBatchProcessingException(Throwable cause) {
        super("Stopping main sink task due to fatal processing error", cause);
    }
}
