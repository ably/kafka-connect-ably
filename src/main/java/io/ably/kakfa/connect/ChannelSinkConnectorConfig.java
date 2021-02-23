package io.ably.kakfa.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;

import io.ably.lib.http.HttpAuth;
import io.ably.lib.transport.Defaults;

import org.apache.kafka.common.config.ConfigDef.Importance;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;

import java.util.Map;

public class ChannelSinkConnectorConfig extends AbstractConfig {

  public static final String CHANNEL_CONFIG = "channel";
  private static final String CHANNEL_CONFIG_DOC = "The ably channel name to use for publishing.";
  private static final String TOPIC_CONFIG = "topic";
  private static final String TOPIC_CONFIG_DOC = "The kafka topic to read from.";

  private static final String CLIENT_ID = "client.id";
  private static final String CLIENT_ID_DOC = "The id of the client represented by this instance. The clientId is " +
    "relevant to presence operations, where the clientId is the principal identifier of the client in presence " +
    "update messages. The clientId is also relevant to authentication; a token issued for a specific client may be " +
    "used to authenticate the bearer of that token to the service.";

  private static final String CLIENT_LOG_LEVEL = "client.loglevel";
  private static final String CLIENT_LOG_LEVEL_DOC = "Log level; controls the level of verbosity of log messages " +
    "from the library.";

  private static final String CLIENT_TLS = "client.tls";
  private static final String CLIENT_TLS_DOC = "Encrypted transport: if true, TLS will be used for all connections " +
    "(whether REST/HTTP or Realtime WebSocket or Comet connections).";

  private static final String CLIENT_REST_HOST = "client.rest.host";
  private static final String CLIENT_REST_HOST_DOC = "For development environments only; allows a non-default Ably " +
    "host to be specified.";

  private static final String CLIENT_REALTIME_HOST = "client.realtime.host";
  private static final String CLIENT_REALTIME_HOST_DOC = "For development environments only; allows a non-default " +
    "Ably host to be specified for websocket connections.";

  private static final String CLIENT_PORT = "client.port";
  private static final String CLIENT_PORT_DOC = "For development environments only; allows a non-default Ably port " +
    "to be specified.";

  private static final String CLIENT_TLS_PORT = "client.tls.port";
  private static final String CLIENT_TLS_PORT_DOC = "For development environments only; allows a non-default Ably " +
    "TLS port to be specified.";

  private static final String CLIENT_AUTO_CONNECT = "client.auto.connect";
  private static final String CLIENT_AUTO_CONNECT_DOC = "If false, suppresses the automatic initiation of a " +
    "connection when the library is instanced.";

  private static final String CLIENT_USE_BINARY_PROTOCOL = "client.use.binary.protocol";
  private static final String CLIENT_USE_BINARY_PROTOCOL_DOC = "If false, forces the library to use the JSON " +
    "encoding for REST and Realtime operations, instead of the default binary msgpack encoding.";

  private static final String CLIENT_QUEUE_MESSAGES = "client.queue.messages";
  private static final String CLIENT_QUEUE_MESSAGES_DOC = "If false, suppresses the default queueing of messages " +
    "when connection states that anticipate imminent connection (connecting and disconnected). Instead, publish " +
    "and presence state changes will fail immediately if not in the connected state.";

  private static final String CLIENT_ECHO_MESSAGES = "client.echo.messages";
  private static final String CLIENT_ECHO_MESSAGES_DOC = "If false, suppresses messages originating from this " +
    "connection being echoed back on the same connection.";

  private static final String CLIENT_RECOVER = "client.recover";
  private static final String CLIENT_RECOVER_DOC = "A connection recovery string, specified by a client when " +
    "initialising the library with the intention of inheriting the state of an earlier connection. See the Ably " +
    "Realtime API documentation for further information on connection state recovery.";

  private static final String CLIENT_PROXY = "client.proxy";
  private static final String CLIENT_PROXY_DOC = "If true, use the configured proxy options to proxy connections.";

  private static final String CLIENT_PROXY_HOST = "client.proxy.host";
  private static final String CLIENT_PROXY_HOST_DOC = "Specifies the client proxy host to use if the client proxy " +
    "is enabled.";

  private static final String CLIENT_PROXY_PORT = "client.proxy.port";
  private static final String CLIENT_PROXY_PORT_DOC = "Specifies the client proxy port to use if the client proxy " +
    "is enabled";

  private static final String CLIENT_PROXY_USERNAME = "client.proxy.username";
  private static final String CLIENT_PROXY_USERNAME_DOC = "Specifies the client proxy username to use if the " +
    "client proxy is enabled.";

  private static final String CLIENT_PROXY_PASSWORD = "client.proxy.password";
  private static final String CLIENT_PROXY_PASSWORD_DOC = "Specifies the client proxy password to use if the " +
    "client proxy is enabled.";

  private static final String CLIENT_PROXY_NON_PROXY_HOSTS = "client.proxy.non.proxy.hosts";
  private static final String CLIENT_PROXY_NON_PROXY_HOSTS_DOC = "Specifies a list of hosts for which the proxy " +
    "should not be used if the client proxy is enabled.";

  private static final String CLIENT_PROXY_PREF_AUTH_TYPE = "client.proxy.pref.auth.type";
  private static final String CLIENT_PROXY_PREF_AUTH_TYPE_DOC = "Specfies the preferred auth type to use if the " +
    "client proxy is enabled. This should be \"BASIC\",  \"DIGEST\" or \"X_ABLY_TOKEN\"";

  private static final String CLIENT_ENVIRONMENT = "client.environment";
  private static final String CLIENT_ENVIRONMENT_DOC = "For development environments only; allows a non-default " +
    "Ably environment to be used such as 'sandbox'. Spec: TO3k1.";

  private static final String CLIENT_IDEMPOTENT_REST_PUBLISHING = "client.idempotent.rest";
  private static final String CLIENT_IDEMPOTENT_REST_PUBLISHING_DOC = "When true idempotent rest publishing will " +
    "be enabled.";

  private static final String CLIENT_HTTP_OPEN_TIMEOUT = "client.http.open.timeout";
  private static final String CLIENT_HTTP_OPEN_TIMEOUT_DOC = "Timeout for opening the http connection";

  private static final String CLIENT_HTTP_REQUEST_TIMEOUT = "client.http.request.timeout";
  private static final String CLIENT_HTTP_REQUEST_TIMEOUT_DOC = "Timeout for any single HTTP request and response";

  private static final String CLIENT_HTTP_MAX_RETRY_COUNT = "client.http.max.retry.count";
  private static final String CLIENT_HTTP_MAX_RETRY_COUNT_DOC = "Max number of fallback hosts to use as a fallback " +
    "when an HTTP request to the primary host is unreachable or indicates that it is unserviceable.";

  private static final String CLIENT_REALTIME_REQUEST_TIMEOUT = "client.realtime.request.timeout";
  private static final String CLIENT_REALTIME_REQUEST_TIMEOUT_DOC = "When a realtime client library is " +
    "establishing a connection with Ably, or sending a HEARTBEAT, CONNECT, ATTACH, DETACH or CLOSE ProtocolMessage " +
    "to Ably, this is the amount of time that the client library will wait before considering that request as " +
    "failed and triggering a suitable failure condition.";

  private static final String CLIENT_FALLBACK_HOSTS = "client.fallback.hosts";
  private static final String CLIENT_FALLBACK_HOSTS_DOC = "List of custom fallback hosts to override the defaults. " +
    "Spec: TO3k6,RSC15a,RSC15b,RTN17b.";

  private static final String CLIENT_TOKEN_PARAMS_TTL = "client.token.params.ttl";
  private static final String CLIENT_TOKEN_PARAMS_TTL_DOC = "Requested time to live for the token in milliseconds. " +
    "When omitted, the REST API default of 60 minutes is applied by Ably.";

  private static final String CLIENT_TOKEN_PARAMS_CAPABILITY = "client.token.params.capability";
  private static final String CLIENT_TOKEN_PARAMS_CAPABILITY_DOC = "Capability requirements JSON stringified for " +
    "the token. When omitted, the REST API default to allow all operations is applied by Ably, with the string " +
    "value {\"*\":[\"*\"]}.";

  private static final String CLIENT_TOKEN_PARAMS_CLIENT_ID = "client.token.params.client.id";
  private static final String CLIENT_TOKEN_PARAMS_CLIENT_ID_DOC = "Requested time to live for the token in " +
    "milliseconds. When omitted, the REST API default of 60 minutes is applied by Ably.";

  private static final String CLIENT_CHANNEL_RETRY_TIMEOUT = "client.channel.retry.timeout";
  private static final String CLIENT_CHANNEL_RETRY_TIMEOUT_DOC = "Channel reattach timeout. Spec: RTL13b.";

  private static final String CLIENT_ASYNC_HTTP_THREADPOOL_SIZE = "client.async.http.threadpool.size";
  private static final String CLIENT_ASYNC_HTTP_THREADPOOL_SIZE_DOC = "Allows the caller to specify a non-default " +
    "size for the asyncHttp threadpool";

  private static final String CLIENT_PUSH_FULL_WAIT = "client.push.full.wait";
  private static final String CLIENT_PUSH_FULL_WAIT_DOC = "Whether to tell Ably to wait for push REST requests to " +
    "fully wait for all their effects before responding.";

  public final String channel;
  public final String topic;

  public ChannelSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.channel = this.getString(CHANNEL_CONFIG);
    this.topic = this.getString(TOPIC_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(CHANNEL_CONFIG, Type.STRING)
                .documentation(CHANNEL_CONFIG_DOC)
                .importance(Importance.HIGH)
                .build()
        )
        .define(
          ConfigKeyBuilder.of(TOPIC_CONFIG, Type.STRING)
            .documentation(TOPIC_CONFIG_DOC)
            .importance(Importance.HIGH)
            .build()
        )
        .define(
          ConfigKeyBuilder.of(CLIENT_ID, Type.STRING)
            .documentation(CLIENT_ID_DOC)
            .importance(Importance.HIGH)
            .build()
        )
        .define(
          ConfigKeyBuilder.of(CLIENT_LOG_LEVEL, Type.INT)
            .documentation(CLIENT_LOG_LEVEL_DOC)
            .importance(Importance.LOW)
            .defaultValue(0)
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
            .defaultValue("")
            .build()
        )
        .define(
          ConfigKeyBuilder.of(CLIENT_REALTIME_HOST, Type.STRING)
            .documentation(CLIENT_REALTIME_HOST_DOC)
            .importance(Importance.LOW)
            .defaultValue("")
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
          ConfigKeyBuilder.of(CLIENT_AUTO_CONNECT, Type.BOOLEAN)
            .documentation(CLIENT_AUTO_CONNECT_DOC)
            .importance(Importance.MEDIUM)
            .defaultValue(true)
            .build()
        )
        .define(
          ConfigKeyBuilder.of(CLIENT_USE_BINARY_PROTOCOL, Type.BOOLEAN)
            .documentation(CLIENT_USE_BINARY_PROTOCOL_DOC)
            .importance(Importance.MEDIUM)
            .defaultValue(true)
            .build()
        )
        .define(
          ConfigKeyBuilder.of(CLIENT_QUEUE_MESSAGES, Type.BOOLEAN)
            .documentation(CLIENT_QUEUE_MESSAGES_DOC)
            .importance(Importance.MEDIUM)
            .defaultValue(true)
            .build()
        )
        .define(
          ConfigKeyBuilder.of(CLIENT_ECHO_MESSAGES, Type.BOOLEAN)
            .documentation(CLIENT_ECHO_MESSAGES_DOC)
            .importance(Importance.MEDIUM)
            .defaultValue(true)
            .build()
        )
        .define(
          ConfigKeyBuilder.of(CLIENT_RECOVER, Type.STRING)
            .documentation(CLIENT_RECOVER_DOC)
            .importance(Importance.MEDIUM)
            .defaultValue("")
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
            .defaultValue("")
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
            .defaultValue("")
            .build()
        )
        .define(
          ConfigKeyBuilder.of(CLIENT_PROXY_PASSWORD, Type.PASSWORD)
            .documentation(CLIENT_PROXY_PASSWORD_DOC)
            .importance(Importance.MEDIUM)
            .defaultValue("")
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
            .defaultValue("")
            .build()
        )
        .define(
          ConfigKeyBuilder.of(CLIENT_ENVIRONMENT, Type.STRING)
            .documentation(CLIENT_ENVIRONMENT_DOC)
            .importance(Importance.LOW)
            .defaultValue("")
            .build()
        )
        .define(
          ConfigKeyBuilder.of(CLIENT_IDEMPOTENT_REST_PUBLISHING, Type.BOOLEAN)
            .documentation(CLIENT_IDEMPOTENT_REST_PUBLISHING_DOC)
            .importance(Importance.MEDIUM)
            .defaultValue(Defaults.ABLY_VERSION_NUMBER >= 1.2)
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
          ConfigKeyBuilder.of(CLIENT_REALTIME_REQUEST_TIMEOUT, Type.LONG)
            .documentation(CLIENT_REALTIME_REQUEST_TIMEOUT_DOC)
            .importance(Importance.MEDIUM)
            .defaultValue(Defaults.realtimeRequestTimeout)
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
          ConfigKeyBuilder.of(CLIENT_CHANNEL_RETRY_TIMEOUT, Type.INT)
            .documentation(CLIENT_CHANNEL_RETRY_TIMEOUT_DOC)
            .importance(Importance.MEDIUM)
            .defaultValue(Defaults.TIMEOUT_CHANNEL_RETRY)
            .build()
        )

        // .CLIENT_TRANSPORT_PARAMS

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

        ;
  }
}

    // /**
    //  * Proxy settings
    //  */
    // public ProxyOptions proxy;

    // public class ProxyOptions {
    //   public String host;
    //   public int port;
    //   public String username;
    //   public String password;
    //   public String[] nonProxyHosts;
    //   public HttpAuth.Type prefAuthType = HttpAuth.Type.BASIC;
    // }


    // /**
    //  * When a TokenParams object is provided, it will override
    //  * the client library defaults described in TokenParams
    //  * Spec: TO3j11
    //  */
    // public TokenParams defaultTokenParams = new TokenParams();


    // /**
    //  * Additional parameters to be sent in the querystring when initiating a realtime connection
    //  */
    // public Param[] transportParams;
