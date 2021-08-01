package com.rbkmoney.fistful.reporter.config;

import com.rbkmoney.testcontainers.annotations.KafkaSpringBootTest;
import com.rbkmoney.testcontainers.annotations.kafka.KafkaTestcontainerSingleton;
import com.rbkmoney.testcontainers.annotations.postgresql.PostgresqlTestcontainerSingleton;
import org.junit.jupiter.api.Disabled;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@PostgresqlTestcontainerSingleton
@KafkaTestcontainerSingleton(
        properties = {
                "kafka.topic.deposit.listener.enabled=true",
                "kafka.topic.destination.listener.enabled=true",
                "kafka.topic.source.listener.enabled=true",
                "kafka.topic.withdrawal.listener.enabled=true",
                "kafka.topic.wallet.listener.enabled=true",
                "kafka.topic.identity.listener.enabled=true"},
        topicsKeys = {
                "kafka.topic.deposit.name",
                "kafka.topic.destination.name",
                "kafka.topic.source.name",
                "kafka.topic.withdrawal.name",
                "kafka.topic.wallet.name",
                "kafka.topic.identity.name"})
@KafkaSpringBootTest
@Disabled
public @interface KafkaPostgresqlSpringBootITest {
}
