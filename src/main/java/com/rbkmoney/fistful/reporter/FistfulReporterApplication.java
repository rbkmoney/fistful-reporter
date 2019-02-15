package com.rbkmoney.fistful.reporter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@ServletComponentScan
@SpringBootApplication(scanBasePackages = {"com.rbkmoney.fistful.reporter"})
@EnableScheduling
public class FistfulReporterApplication {
    /*asdasd*/
    public static void main(String[] args) {
        SpringApplication.run(FistfulReporterApplication.class, args);
    }

}
