package com.ably.kafka.connect;

import com.ably.kafka.connect.batch.BatchProcessingThread;
import com.ably.kafka.connect.client.AblyClient;
import com.ably.kafka.connect.client.AblyClientFactory;
import com.ably.kafka.connect.client.DefaultAblyBatchClient;
import com.ably.kafka.connect.client.DefaultAblyClientFactory;
import com.ably.kafka.connect.config.ChannelSinkConnectorConfig;
import com.ably.kafka.connect.config.KafkaRecordErrorReporter;
import com.ably.kafka.connect.offset.OffsetRegistryService;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.ably.lib.types.AblyException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ChannelSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(ChannelSinkTask.class);

    private AblyClientFactory ablyClientFactory = new DefaultAblyClientFactory();
    private DefaultAblyBatchClient ablyClient;

    private final BlockingQueue<Runnable> sinkRecordsQueue = new LinkedBlockingQueue<>();
    private ThreadPoolExecutor executor;

    private int maxBufferLimit = 0;

    public ChannelSinkTask() {}

    @VisibleForTesting
    ChannelSinkTask(AblyClientFactory factory) {
        this.ablyClientFactory = factory;
    }

    @VisibleForTesting
    AblyClient getAblyClient() {
        return ablyClient;
    }

    private final OffsetRegistryService offsetRegistryService = new OffsetRegistryService();

    @Override
    public void start(Map<String, String> settings) {
        logger.info("Starting Ably channel Sink task");

        try {
            this.ablyClient = (DefaultAblyBatchClient) this.ablyClientFactory.create(settings);
        } catch (ChannelSinkConnectorConfig.ConfigException | AblyException e) {
            throw new RuntimeException(e);
        }

        final int maxThreadPoolSize = Integer.parseInt(settings.getOrDefault
                (ChannelSinkConnectorConfig.BATCH_EXECUTION_THREAD_POOL_SIZE,
                        ChannelSinkConnectorConfig.BATCH_EXECUTION_THREAD_POOL_SIZE_DEFAULT));

        this.executor = new ThreadPoolExecutor(maxThreadPoolSize, maxThreadPoolSize, 30,
                TimeUnit.SECONDS, sinkRecordsQueue,
                new ThreadPoolExecutor.CallerRunsPolicy());

        this.maxBufferLimit = Integer.parseInt(settings.getOrDefault(ChannelSinkConnectorConfig.BATCH_EXECUTION_MAX_BUFFER_SIZE,
                ChannelSinkConnectorConfig.BATCH_EXECUTION_MAX_BUFFER_SIZE_DEFAULT));


    }

    // Local buffer of records.
    //List<SinkRecord> bufferedRecords = new ArrayList<SinkRecord>();
    @Override
    public void put(Collection<SinkRecord> records) {

        if(records.size() > 0) {
            logger.debug("SinkTask put - Num records: " + records.size());

            Lists.partition(new ArrayList<>(records), this.maxBufferLimit).forEach(batch -> {
                this.executor.execute(new BatchProcessingThread(batch, this.ablyClient, this.offsetRegistryService));
            });
        }
    }

    /**
     * precommit is called in regular intervals by the kafka connect
     * framework. We store that the offsets that are successfully processed
     * in the offsetRegistryService. This is used to commit the offsets
     * precommit automatically calls flush.
     * @param offsets
     * @return
     * @throws RetriableException
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets) throws RetriableException {

        // The offsets map will contain offsets of other topics and partitions
        // that are not relevant to this sink task. We need to filter them out.
        return this.offsetRegistryService.updateOffsets(offsets);
    }

    @Override
    public void stop() {
        logger.info("Stopping Ably channel Sink task");

        if(this.executor != null) {
            this.executor.shutdown();
        }

    }

    static KafkaRecordErrorReporter noOpKafkaRecordErrorReporter() {
        return (record, e) -> {};
    }


    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
