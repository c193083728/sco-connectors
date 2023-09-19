package com.github.sco1237896.connector.it.support

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j
import org.assertj.core.api.SoftAssertionsProvider
import org.awaitility.Awaitility
import org.bson.types.ObjectId
import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.Slf4jLogConsumer
import spock.lang.Specification

import java.util.concurrent.TimeUnit

@Slf4j
abstract class ConnectorSpecSupport extends Specification {

    static Slf4jLogConsumer logger(String name) {
        new Slf4jLogConsumer(LoggerFactory.getLogger(name))
    }

    static void await(long timeout, TimeUnit unit, Closure<Boolean> condition) {
        until(timeout, unit, condition)
    }

    static void await(long timeout, long poll, TimeUnit unit, Closure<Boolean> condition) {
        Awaitility.await()
                .atMost(timeout, unit)
                .pollDelay(poll, unit)
                .until(() -> condition())
    }

    static void until(long timeout, TimeUnit unit, Closure<Boolean> condition) {
        Awaitility.await()
                .atMost(timeout, unit)
                .pollDelay(250, TimeUnit.MILLISECONDS)
                .until(() -> condition())
    }

    static void untilAsserted(long timeout, TimeUnit unit, SoftAssertionsProvider.ThrowingRunnable condition) {
        Awaitility.await()
                .atMost(timeout, unit)
                .pollDelay(250, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> condition())
    }

    static void untilAsserted(long timeout, long poll, TimeUnit unit, SoftAssertionsProvider.ThrowingRunnable condition) {
        Awaitility.await()
                .atMost(timeout, unit)
                .pollDelay(poll, unit)
                .untilAsserted(() -> condition())
    }

    static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return
        }

        try {
            closeable.close()
        } catch (Exception e) {
            log.warn('Failed to close {}', closeable, e)
        }
    }

    static String json(Object content) {
        new JsonBuilder(content).toString()
    }

    static boolean hasEnv(String envName) {
        String value = System.getenv(envName)

        if (value == null) {
            return false
        }

        return value.trim().length() != 0
    }

    static String uid() {
        return ObjectId.get().toString();
    }

    static String uid(String prefix) {
        return prefix + uid();
    }
}
