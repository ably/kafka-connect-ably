package com.ably.kafka.connect.client;

import java.util.Objects;

/**
 * Exception to capture an error submitting messages to a particular Ably chanel
 */
public class AblyChannelPublishException extends Exception {

    public final String channelName;
    public final int errorCode;
    public final int statusCode;

    public AblyChannelPublishException(String channelName, int errorCode, int statusCode, String message) {
        super(message);
        this.channelName = channelName;
        this.errorCode = errorCode;
        this.statusCode = statusCode;
   }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AblyChannelPublishException that = (AblyChannelPublishException) o;
        return errorCode == that.errorCode && statusCode == that.statusCode && Objects.equals(channelName, that.channelName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelName, errorCode, statusCode);
    }

    @Override
    public String toString() {
        return "AblyChannelPublishException{" +
            "channelName='" + channelName + '\'' +
            ", errorCode=" + errorCode +
            ", statusCode=" + statusCode +
            '}';
    }
}
