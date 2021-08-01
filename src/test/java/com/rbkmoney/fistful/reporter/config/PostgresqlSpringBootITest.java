package com.rbkmoney.fistful.reporter.config;

import com.rbkmoney.fistful.reporter.config.testconfiguration.WithdrawalTestDaoConfig;
import com.rbkmoney.testcontainers.annotations.DefaultSpringBootTest;
import com.rbkmoney.testcontainers.annotations.postgresql.PostgresqlTestcontainerSingleton;
import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@PostgresqlTestcontainerSingleton
@DefaultSpringBootTest
@Import(WithdrawalTestDaoConfig.class)
public @interface PostgresqlSpringBootITest {
}
