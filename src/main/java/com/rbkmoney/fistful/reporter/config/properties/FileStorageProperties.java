package com.rbkmoney.fistful.reporter.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.time.ZoneId;

@Configuration
@ConfigurationProperties(prefix = "filestorage")
@Data
public class FileStorageProperties {

    private Resource url;
    private int clientTimeout;
    private Long urlLifeTimeDuration;
    private ZoneId timeZone;
    private String cephEndpoint;

}
