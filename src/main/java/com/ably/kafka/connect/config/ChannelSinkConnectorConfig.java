
package com.ably.kafka.connect.config;

import com.ably.kafka.connect.validators.ChannelNameValidator;
import com.ably.kafka.connect.validators.MultiConfigValidator;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import io.ably.lib.http.HttpAuth;
import io.ably.lib.rest.Auth.TokenParams;
import io.ably.lib.transport.Defaults;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.ClientOptions;
import io.ably.lib.types.ProxyOptions;
import io.ably.lib.util.Log;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ChannelSinkConnectorConfig extends AbstractConfig {

    public static final String CHANNEL_CONFIG = "channel";
    private static final String CHANNEL_CONFIG_DOC = "The ably channel name to use for publishing.";
    public static final String MESSAGE_CONFIG = "message.name";
    private static final String MESSAGE_CONFIG_DOC = "Ably message name to use for publishing.";

    public static final String CLIENT_KEY = "client.key";
    private static final String CLIENT_KEY_DOC = "The Ably API key string. The key string is obtained from the " +
        "application dashboard.";

    public static final String CLIENT_ID = "client.id";
    private static final String CLIENT_ID_DOC = "The id of the client represented by this instance. The clientId is " +
        "relevant to presence operations, where the clientId is the principal identifier of the client in presence " +
        "update messages. The clientId is also relevant to authentication; a token issued for a specific client may be " +
        "used to authenticate the bearer of that token to the service.";

    public static final String CLIENT_LOG_LEVEL = "client.loglevel";
    private static final String CLIENT_LOG_LEVEL_DOC = "Log level; controls the level of verbosity of log messages " +
        "from the library.";

    public static final String CLIENT_TLS = "client.tls";
    private static final String CLIENT_TLS_DOC = "Encrypted transport: if true, TLS will be used for all connections " +
        "(whether REST/HTTP or Realtime WebSocket or Comet connections).";

    public static final String CLIENT_REST_HOST = "client.rest.host";
    private static final String CLIENT_REST_HOST_DOC = "For development environments only; allows a non-default Ably " +
        "host to be specified.";

    public static final String CLIENT_PORT = "client.port";
    private static final String CLIENT_PORT_DOC = "For development environments only; allows a non-default Ably port " +
        "to be specified.";

    public static final String CLIENT_TLS_PORT = "client.tls.port";
    private static final String CLIENT_TLS_PORT_DOC = "For development environments only; allows a non-default Ably " +
        "TLS port to be specified.";

    public static final String CLIENT_PROXY = "client.proxy";
    private static final String CLIENT_PROXY_DOC = "If true, use the configured proxy options to proxy connections.";

    public static final String CLIENT_PROXY_HOST = "client.proxy.host";
    private static final String CLIENT_PROXY_HOST_DOC = "Specifies the client proxy host to use if the client proxy " +
        "is enabled.";

    public static final String CLIENT_PROXY_PORT = "client.proxy.port";
    private static final String CLIENT_PROXY_PORT_DOC = "Specifies the client proxy port to use if the client proxy " +
        "is enabled";

    public static final String CLIENT_PROXY_USERNAME = "client.proxy.username";
    private static final String CLIENT_PROXY_USERNAME_DOC = "Specifies the client proxy username to use if the " +
        "client proxy is enabled.";

    public static final String CLIENT_PROXY_PASSWORD = "client.proxy.password";
    private static final String CLIENT_PROXY_PASSWORD_DOC = "Specifies the client proxy password to use if the " +
        "client proxy is enabled.";

    public static final String CLIENT_PROXY_NON_PROXY_HOSTS = "client.proxy.non.proxy.hosts";
    private static final String CLIENT_PROXY_NON_PROXY_HOSTS_DOC = "Specifies a list of hosts for which the proxy " +
        "should not be used if the client proxy is enabled.";

    public static final String CLIENT_PROXY_PREF_AUTH_TYPE = "client.proxy.pref.auth.type";
    private static final String CLIENT_PROXY_PREF_AUTH_TYPE_DOC = "Specfies the preferred auth type to use if the " +
        "client proxy is enabled. This should be \"BASIC\",  \"DIGEST\" or \"X_ABLY_TOKEN\"";

    public static final String CLIENT_ENVIRONMENT = "client.environment";
    private static final String CLIENT_ENVIRONMENT_DOC = "For development environments only; allows a non-default " +
        "Ably environment to be used such as 'sandbox'. Spec: TO3k1.";

    public static final String CLIENT_HTTP_OPEN_TIMEOUT = "client.http.open.timeout";
    private static final String CLIENT_HTTP_OPEN_TIMEOUT_DOC = "Timeout for opening the http connection";

    public static final String CLIENT_HTTP_REQUEST_TIMEOUT = "client.http.request.timeout";
    private static final String CLIENT_HTTP_REQUEST_TIMEOUT_DOC = "Timeout for any single HTTP request and response";

    public static final String CLIENT_HTTP_MAX_RETRY_COUNT = "client.http.max.retry.count";
    private static final String CLIENT_HTTP_MAX_RETRY_COUNT_DOC = "Max number of fallback hosts to use as a fallback " +
        "when an HTTP request to the primary host is unreachable or indicates that it is unserviceable.";

    public static final String CLIENT_FALLBACK_HOSTS = "client.fallback.hosts";
    private static final String CLIENT_FALLBACK_HOSTS_DOC = "List of custom fallback hosts to override the defaults. " +
        "Spec: TO3k6,RSC15a,RSC15b,RTN17b.";

    public static final String CLIENT_TOKEN_PARAMS = "client.token.params";
    private static final String CLIENT_TOKEN_PARAMS_DOC = "If true, use the configured token params.";

    public static final String CLIENT_TOKEN_PARAMS_TTL = "client.token.params.ttl";
    private static final String CLIENT_TOKEN_PARAMS_TTL_DOC = "Requested time to live for the token in milliseconds. " +
        "When omitted, the REST API default of 60 minutes is applied by Ably. Client token params must be enabled";

    public static final String CLIENT_TOKEN_PARAMS_CAPABILITY = "client.token.params.capability";
    private static final String CLIENT_TOKEN_PARAMS_CAPABILITY_DOC = "Capability requirements JSON stringified for " +
        "the token. When omitted, the REST API default to allow all operations is applied by Ably, with the string " +
        "value {\"*\":[\"*\"]}. Client token params must be enabled.";

    public static final String CLIENT_TOKEN_PARAMS_CLIENT_ID = "client.token.params.client.id";
    private static final String CLIENT_TOKEN_PARAMS_CLIENT_ID_DOC = "Requested time to live for the token in " +
        "milliseconds. When omitted, the REST API default of 60 minutes is applied by Ably. Client token params must " +
        "be enabled.";

    public static final String CLIENT_ASYNC_HTTP_THREADPOOL_SIZE = "client.async.http.threadpool.size";
    private static final String CLIENT_ASYNC_HTTP_THREADPOOL_SIZE_DOC = "Allows the caller to specify a non-default " +
        "size for the asyncHttp threadpool";

    public static final String CLIENT_PUSH_FULL_WAIT = "client.push.full.wait";
    private static final String CLIENT_PUSH_FULL_WAIT_DOC = "Whether to tell Ably to wait for push REST requests to " +
        "fully wait for all their effects before responding.";

    public static final String MESSAGE_PAYLOAD_SIZE_MAX = "messagePayloadSizeMax";
    // max payload size in bytes(64KB)
    public static final int MESSAGE_PAYLOAD_SIZE_MAX_DEFAULT = 64 * 1024;
    private static final String MESSAGE_PAYLOAD_SIZE_MAX_DOC = "Maximum size of the message payload in KB";

    public static final String SKIP_ON_KEY_ABSENCE = "skipOnKeyAbsence";
    private static final String SKIP_ON_KEY_ABSENCE_DOC = "If true, it skips the record if the key has been provided as" +
        " part of interpolable configuration value, but key is not available on the time of record creation. Default value is false.";

    public static final String BATCH_EXECUTION_THREAD_POOL_SIZE = "batchExecutionThreadPoolSize";
    private static final String BATCH_EXECUTION_THREAD_POOL_SIZE_DOC = "Size of Thread pool that is used to batch " +
    "the records and call Ably REST API(Batch)";
    public static final String BATCH_EXECUTION_THREAD_POOL_SIZE_DEFAULT = "10";

    public static final String BATCH_EXECUTION_MAX_BUFFER_SIZE = "batchExecutionMaxBufferSize";
    public static final String BATCH_EXECUTION_MAX_BUFFER_SIZE_DEFAULT = "1000";
    private static final String BATCH_EXECUTION_MAX_BUFFER_SIZE_DOC = "Size of the buffer, records " +
        "are buffered or chunked before calling the Ably Batch REST API";

    private static final String BATCH_EXECUTION_MAX_BUFFER_SIZE_DOC = "Size of the buffer, records " +
        "are buffered or chunked before calling the Ably Batch REST API";

    public static final String BATCH_EXECUTION_MAX_BUFFER_DELAY_MS = "batchExecutionMaxBufferSizeMs";
    public static final String BATCH_EXECUTION_MAX_BUFFER_DELAY_MS_DEFAULT = "5000";
    public static final String BATCH_EXECUTION_MAX_BUFFER_DELAY_MS_DOC =
        "Maximum delay to buffer records before submitting records collected so far to Ably";



    // The name of the extra agent identifier to add to the Ably-Agent header to
    // identify this client as using the Ably Kafka Connector.
    private static final String ABLY_AGENT_HEADER_NAME = "kafka-connect-ably";

    // The default Ably-Agent version to use when the library version can't be
    // determined (for example in the tests).
    private static final String ABLY_AGENT_DEFAULT_VERSION = "0.0.0";

    private static final Logger logger = LoggerFactory.getLogger(ChannelSinkConnectorConfig.class);

    public final ClientOptions clientOptions;

    public static class ConfigException extends Exception {
        private static final long serialVersionUID = 6225540388729441285L;

        public ConfigException(String message) {
            super(message);
        }

        public ConfigException(String message, Exception cause) {
            super(message, cause);
        }
    }

    public ChannelSinkConnectorConfig(Map<?, ?> originals) {
        super(createConfig(), originals);

        ClientOptions clientOpts = null;
        try {
            clientOpts = getAblyClientOptions();
        } catch (ConfigException | AblyException e) {
            logger.error("Error configuring Ably client options", e);
        }
        clientOptions = clientOpts;
    }

    private ClientOptions getAblyClientOptions() throws AblyException, ConfigException {
        ClientOptions opts = new ClientOptions(getPassword(CLIENT_KEY).value());

        opts.clientId = getString(CLIENT_ID);
        opts.logLevel = getInt(CLIENT_LOG_LEVEL);
        opts.tls = getBoolean(CLIENT_TLS);
        opts.restHost = getString(CLIENT_REST_HOST);
        opts.port = getInt(CLIENT_PORT);
        opts.tlsPort = getInt(CLIENT_TLS_PORT);
        if (getBoolean(CLIENT_PROXY)) {
            ProxyOptions proxyOpts = new ProxyOptions();
            proxyOpts.host = getString(CLIENT_PROXY_HOST);
            proxyOpts.port = getInt(CLIENT_PROXY_PORT);
            proxyOpts.username = getString(CLIENT_PROXY_USERNAME);
            proxyOpts.password = getPassword(CLIENT_PROXY_PASSWORD) != null ? getPassword(CLIENT_PROXY_PASSWORD).value() : null;
            proxyOpts.nonProxyHosts = getList(CLIENT_PROXY_NON_PROXY_HOSTS) != null ? getList(CLIENT_PROXY_NON_PROXY_HOSTS).toArray(new String[0]) : null;
            proxyOpts.prefAuthType = HttpAuth.Type.valueOf(getString(CLIENT_PROXY_PREF_AUTH_TYPE));
            opts.proxy = proxyOpts;
        }
        opts.environment = getString(CLIENT_ENVIRONMENT);
        opts.httpOpenTimeout = getInt(CLIENT_HTTP_OPEN_TIMEOUT);
        opts.httpRequestTimeout = getInt(CLIENT_HTTP_REQUEST_TIMEOUT);
        opts.httpMaxRetryCount = getInt(CLIENT_HTTP_MAX_RETRY_COUNT);
        opts.fallbackHosts = getList(CLIENT_FALLBACK_HOSTS).toArray(new String[0]);
        if (getBoolean(CLIENT_TOKEN_PARAMS)) {
            TokenParams tokenParams = new TokenParams();
            tokenParams.ttl = getLong(CLIENT_TOKEN_PARAMS_TTL);
            tokenParams.capability = getString(CLIENT_TOKEN_PARAMS_CAPABILITY);
            tokenParams.clientId = getString(CLIENT_TOKEN_PARAMS_CLIENT_ID);
        }
        opts.asyncHttpThreadpoolSize = getInt(CLIENT_ASYNC_HTTP_THREADPOOL_SIZE);
        opts.pushFullWait = getBoolean(CLIENT_PUSH_FULL_WAIT);

        // Add the library version to the list of Ably-Agent identifiers.
        String version = getClass().getPackage().getImplementationVersion();
        if (version == null) {
            version = ABLY_AGENT_DEFAULT_VERSION;
        }
        opts.agents = Map.of(ABLY_AGENT_HEADER_NAME, version);

        return opts;
    }

    public static ConfigDef createConfig() {
        return new ConfigDef()
            .define(
                ConfigKeyBuilder.of(CHANNEL_CONFIG, Type.STRING)
                    .documentation(CHANNEL_CONFIG_DOC)
                    .importance(Importance.HIGH)
                    .validator(new MultiConfigValidator(new ConfigDef.Validator[]{
                        new ConfigDef.NonNullValidator(),
                        new ConfigDef.NonEmptyString(),
                        new ChannelNameValidator()
                    }))
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_KEY, Type.PASSWORD)
                    .documentation(CLIENT_KEY_DOC)
                    .importance(Importance.HIGH)
                    .validator(new ConfigDef.NonNullValidator())
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_ID, Type.STRING)
                    .documentation(CLIENT_ID_DOC)
                    .importance(Importance.HIGH)
                    .validator(new MultiConfigValidator(new ConfigDef.Validator[]{
                        new ConfigDef.NonNullValidator(),
                        new ConfigDef.NonEmptyString()
                    }))
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(MESSAGE_CONFIG, Type.STRING)
                    .documentation(MESSAGE_CONFIG_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(null)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_LOG_LEVEL, Type.INT)
                    .documentation(CLIENT_LOG_LEVEL_DOC)
                    .importance(Importance.LOW)
                    .defaultValue(Log.VERBOSE)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_TLS, Type.BOOLEAN)
                    .documentation(CLIENT_TLS_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(true)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_REST_HOST, Type.STRING)
                    .documentation(CLIENT_REST_HOST_DOC)
                    .importance(Importance.LOW)
                    .defaultValue(null)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_PORT, Type.INT)
                    .documentation(CLIENT_PORT_DOC)
                    .importance(Importance.LOW)
                    .defaultValue(0)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_TLS_PORT, Type.INT)
                    .documentation(CLIENT_TLS_PORT_DOC)
                    .importance(Importance.LOW)
                    .defaultValue(0)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_PROXY, Type.BOOLEAN)
                    .documentation(CLIENT_PROXY_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(false)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_PROXY_HOST, Type.STRING)
                    .documentation(CLIENT_PROXY_HOST_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(null)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_PROXY_PORT, Type.INT)
                    .documentation(CLIENT_PROXY_PORT_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(0)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_PROXY_USERNAME, Type.STRING)
                    .documentation(CLIENT_PROXY_USERNAME_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(null)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_PROXY_PASSWORD, Type.PASSWORD)
                    .documentation(CLIENT_PROXY_PASSWORD_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(null)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_PROXY_PREF_AUTH_TYPE, Type.STRING)
                    .documentation(CLIENT_PROXY_PREF_AUTH_TYPE_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(HttpAuth.Type.BASIC.name())
                    .validator(Validators.validEnum(HttpAuth.Type.class))
                    .recommender(Recommenders.enumValues(HttpAuth.Type.class))
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_PROXY_NON_PROXY_HOSTS, Type.LIST)
                    .documentation(CLIENT_PROXY_NON_PROXY_HOSTS_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(null)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_ENVIRONMENT, Type.STRING)
                    .documentation(CLIENT_ENVIRONMENT_DOC)
                    .importance(Importance.LOW)
                    .defaultValue(null)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_HTTP_OPEN_TIMEOUT, Type.INT)
                    .documentation(CLIENT_HTTP_OPEN_TIMEOUT_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(Defaults.TIMEOUT_HTTP_OPEN)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_HTTP_REQUEST_TIMEOUT, Type.INT)
                    .documentation(CLIENT_HTTP_REQUEST_TIMEOUT_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(Defaults.TIMEOUT_HTTP_REQUEST)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_HTTP_MAX_RETRY_COUNT, Type.INT)
                    .documentation(CLIENT_HTTP_MAX_RETRY_COUNT_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(Defaults.HTTP_MAX_RETRY_COUNT)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_FALLBACK_HOSTS, Type.LIST)
                    .documentation(CLIENT_FALLBACK_HOSTS_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue("")
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_TOKEN_PARAMS, Type.BOOLEAN)
                    .documentation(CLIENT_TOKEN_PARAMS_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(false)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_TOKEN_PARAMS_TTL, Type.LONG)
                    .documentation(CLIENT_TOKEN_PARAMS_TTL_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(0L)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_TOKEN_PARAMS_CAPABILITY, Type.STRING)
                    .documentation(CLIENT_TOKEN_PARAMS_CAPABILITY_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue("")
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_TOKEN_PARAMS_CLIENT_ID, Type.LONG)
                    .documentation(CLIENT_TOKEN_PARAMS_CLIENT_ID_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(0L)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_ASYNC_HTTP_THREADPOOL_SIZE, Type.INT)
                    .documentation(CLIENT_ASYNC_HTTP_THREADPOOL_SIZE_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(Defaults.HTTP_ASYNC_THREADPOOL_SIZE)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(CLIENT_PUSH_FULL_WAIT, Type.BOOLEAN)
                    .documentation(CLIENT_PUSH_FULL_WAIT_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(false)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(SKIP_ON_KEY_ABSENCE, Type.BOOLEAN)
                    .documentation(SKIP_ON_KEY_ABSENCE_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(false)
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(BATCH_EXECUTION_THREAD_POOL_SIZE, Type.INT)
                    .documentation(BATCH_EXECUTION_THREAD_POOL_SIZE_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(Integer.parseInt(BATCH_EXECUTION_THREAD_POOL_SIZE_DEFAULT))
                    .build()
            )
            .define(
                ConfigKeyBuilder.of(MESSAGE_PAYLOAD_SIZE_MAX, Type.INT)
                    .documentation(MESSAGE_PAYLOAD_SIZE_MAX_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(MESSAGE_PAYLOAD_SIZE_MAX_DEFAULT)
                    .build()
            )
            .define(
               ConfigKeyBuilder.of(BATCH_EXECUTION_MAX_BUFFER_SIZE, Type.INT)
                       .documentation(BATCH_EXECUTION_MAX_BUFFER_SIZE_DOC)
                       .importance(Importance.MEDIUM)
                       .defaultValue(Integer.parseInt(BATCH_EXECUTION_MAX_BUFFER_SIZE_DEFAULT))
                       .build()
            )
            .define(
                ConfigKeyBuilder.of(BATCH_EXECUTION_MAX_BUFFER_DELAY_MS, Type.INT)
                    .documentation(BATCH_EXECUTION_MAX_BUFFER_DELAY_MS_DOC)
                    .importance(Importance.MEDIUM)
                    .defaultValue(Integer.parseInt(BATCH_EXECUTION_MAX_BUFFER_DELAY_MS_DEFAULT))
                    .build()
            );
    }
}
