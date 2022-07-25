package com.couchbase.couchbaserestclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.couchbase.couchbaserestclient"})
public class CouchbaseRestClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(CouchbaseRestClientApplication.class, args);
    }

}
