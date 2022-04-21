package com.ably.kafka.connect.config;

import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class ExampleCipherConfig implements CipherConfig {
    @Override
    public String key(SinkRecord record) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(record.topic().getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;

    }
}
