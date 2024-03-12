package com.ably.kafka.connect.batch;

import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.mapping.MessageConverter;
import com.ably.kafka.connect.mapping.RecordMapping;
import com.ably.kafka.connect.mapping.RecordMappingException;
import io.ably.lib.types.Message;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class MessageTransformer {
    private static final Logger logger = LoggerFactory.getLogger(MessageTransformer.class);

    private final RecordMapping channelMapping;
    private final RecordMapping messageNameMapping;
    private final ChannelSinkConnectorConfig.FailedRecordMappingAction actionOnFailure;
    @Nullable
    private final ErrantRecordReporter dlqReporter;

    /**
     * Construct a new message transformer, for generating Ably BatchSpecs and converting
     * records to messages as needed.
     *
     * @param channelMapping     The RecordMapping to use to generate Ably channel names
     * @param messageNameMapping The RecordMapping to use to generate Ably Message names
     * @param actionOnFailure    Action to perform when a message mapping attempt fails
     * @param dlqReporter        dead letter queue for reporting bad records, or null if not in use
     */
    public MessageTransformer(
        RecordMapping channelMapping,
        RecordMapping messageNameMapping,
        ChannelSinkConnectorConfig.FailedRecordMappingAction actionOnFailure,
        @Nullable ErrantRecordReporter dlqReporter) {
        this.channelMapping = channelMapping;
        this.messageNameMapping = messageNameMapping;
        this.actionOnFailure = actionOnFailure;
        this.dlqReporter = dlqReporter;
    }

    /**
     * Construct Ably messages for an incoming batch of Kafka records
     *
     * @param records Kafka sink records to transform to Ably messages
     * @return List of Kafka sink records with transformed Ably messages
     * @throws FatalBatchProcessingException if a fatal error occurred processing records
     */
    public List<RecordMessagePair> transform(List<SinkRecord> records) throws FatalBatchProcessingException {
        return records.stream().map(record -> {
            try {
                String channel = channelMapping.map(record);
                String messageName = messageNameMapping.map(record);
                Message message = MessageConverter.toAblyMessage(messageName, record);
                return new RecordMessagePair(record, message, channel);
            } catch (RecordMappingException mappingError) {
                handleMappingFailure(record, mappingError);
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }


    /**
     * Process a record that we're unable to forward to Ably due to a failed channel or
     * message name mapping according to the configured handling behaviour.
     *
     * @param record       The SinkRecord we weren't able to map
     * @param mappingError The error raised by the RecordMapping
     */
    private void handleMappingFailure(
        final SinkRecord record,
        final RecordMappingException mappingError) {
        switch (actionOnFailure) {
            case STOP_TASK:
                logger.error("Stopping task due to mapping failure with record {}", record, mappingError);
                throw new FatalBatchProcessingException(mappingError);
            case SKIP_RECORD:
                logger.debug("Skipping record {} due to mapping failure", record, mappingError);
                break;
            case DLQ_RECORD:
                logger.debug("Sending record {} to DLQ due to mapping failure", record, mappingError);
                if (dlqReporter != null) {
                    dlqReporter.report(record, mappingError);
                } else {
                    logger.error("Unable to send record {} to DLQ as it is not configured. Stopping task!",
                        record, mappingError);
                    throw new FatalBatchProcessingException(mappingError);
                }
                break;
        }
    }

}
