/**
 * Copyright © 2021 Ably Real-time Ltd. (support@ably.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ably.kafka.connect;

import java.util.List;
import java.util.ArrayList;

import io.ably.pubsub.http.HttpRequester;
import io.ably.pubsub.http.HttpCore;
import io.ably.pubsub.http.HttpScheduler;
import io.ably.pubsub.http.HttpUtils;
import io.ably.pubsub.http.HttpHelpers;
import io.ably.pubsub.realtime.PubSubRealtimeClient;
import io.ably.pubsub.realtime.Channel;
import io.ably.pubsub.server.PubSubServer;
import io.ably.pubsub.realtime.ChannelBase.MessageListener;
import io.ably.pubsub.http.PubSubHttpClient;
import io.ably.pubsub.types.AblyException;
import io.ably.pubsub.types.Callback;
import io.ably.pubsub.types.ClientOptions;
import io.ably.pubsub.types.ErrorInfo;
import io.ably.pubsub.types.Message;
import io.ably.pubsub.util.Serialisation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for interacting with the Ably Sandbox environment.
 *
 * Largely copied from ably-java which currently doesn't expose the code in a
 * module that can be imported:
 *
 * https://github.com/ably/ably-java/blob/main/lib/src/test/java/io/ably/lib/test/common/Setup.java
 */
public class AblyHelpers {
    public static final String TEST_ENDPOINT = "nonprod:sandbox";

    public static class Key {
        public String keyName;
        public String keySecret;
        public String keyStr;
        public String capability;
        public Integer status;
    }

    public static class AppSpec {
        public String id;
        public String appId;
        public String accountId;
        public Key[] keys;

        public String key() {
            return this.keys[0].keyStr;
        }
    }

    /**
     * Create a test app in Ably Sandbox by sending a POST request with an app
     * spec in the body to /apps.
     */
    public static AppSpec createTestApp() throws AblyException {
        ClientOptions opts = new ClientOptions("a.b:c");
        opts.endpoint = TEST_ENDPOINT;
        PubSubRealtimeClient ably = PubSubServer.realtimeClientBuilder(opts).build();

        AppSpec request = new AppSpec();
        request.keys = new Key[]{new Key()};

        return HttpHelpers.postSync(ably.http, "/apps", null, null, new HttpUtils.JsonRequestBody(request), new HttpCore.ResponseHandler<AppSpec>() {
            @Override
            public AppSpec handleResponse(HttpCore.Response response, ErrorInfo error) throws AblyException {
                if(error != null) {
                    throw AblyException.fromErrorInfo(error);
                }
                return (AppSpec)Serialisation.gson.fromJson(new String(response.body), AppSpec.class);
            }
        }, false);
    }

    /**
     * Delete a test app in Ably Sandbox previously created via createTestApp.
     */
    public static void deleteTestApp(AppSpec appSpec) {
        try {
            ClientOptions opts = new ClientOptions(appSpec.key());
            opts.endpoint = TEST_ENDPOINT;
            PubSubHttpClient ably = PubSubServer.httpClientBuilder(opts).build();

            ably.http.request(new HttpRequester.Execute<Void>() {
                @Override
                public void execute(HttpScheduler http, Callback<Void> callback) throws AblyException {
                    http.del("/apps/" + appSpec.appId, HttpUtils.defaultAcceptHeaders(false), null, null, true, callback);
                }
            }).sync();
        } catch (AblyException e) {
            System.err.println("Unable to delete Ably test app: " + e);
            e.printStackTrace();
        }
    }

    /**
     * Return a realtime client using the key from the given app spec.
     */
    public static PubSubRealtimeClient realtimeClient(AppSpec appSpec) throws AblyException {
        ClientOptions opts = new ClientOptions(appSpec.key());
        opts.endpoint = TEST_ENDPOINT;
        return PubSubServer.realtimeClientBuilder(opts).build();
    }

    /**
     * A class that subscribes to a channel and tracks messages received.
     * @author paddy
     *
     */
    public static class MessageWaiter implements MessageListener {
        public List<Message> receivedMessages;

        /**
         * Track all messages on a channel.
         * @param channel
         */
        public MessageWaiter(Channel channel) {
            reset();
            try {
                channel.subscribe(this);
            } catch(AblyException e) {

            }
        }

        /**
         * Wait for a given interval for a number of messages
         * @param count
         */
        public synchronized void waitFor(int count, long time) {
            long targetTime = System.currentTimeMillis() + time;
            long remaining = time;
            while(receivedMessages.size() < count && remaining > 0) {
                try {
                    wait(remaining);
                } catch(InterruptedException e) {
                }
                remaining = targetTime - System.currentTimeMillis();
            }
        }

        /**
         * Reset the counter. Waiters will continue to
         * wait, and will be unblocked when the revised count
         * meets their requirements.
         */
        public synchronized void reset() {
            receivedMessages = new ArrayList<Message>();
        }

        /**
         * MessageListener interface
         */
        @Override
        public void onMessage(Message message) {
            synchronized(this) {
                receivedMessages.add(message);
                notify();
            }
        }
    }
}
