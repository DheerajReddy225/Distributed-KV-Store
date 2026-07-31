package com.kvstore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling  // needed for periodic health checks
public class KvStoreApplication {
    public static void main(String[] args) {
        SpringApplication.run(KvStoreApplication.class, args);
    }
}
