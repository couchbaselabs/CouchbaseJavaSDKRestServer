package com.couchbase.couchbaserestclient.sdk;

import reactor.util.annotation.Nullable;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonGetter;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

public class Result {
    private final String id;
    private final Object document;
    private final @Nullable Throwable err;
    private final boolean status;


    @JsonCreator
    public Result(@JsonProperty("id") String id, @JsonProperty("document") Object document,
                  @JsonProperty("err") Throwable err, @JsonProperty("status") boolean status) {
      this.id = id;
      this.document = document;
      this.err = err;
      this.status = status;
    }

    @JsonGetter
    public String id() {
      return id;
    }

    @JsonGetter
    public Object document() {
      return document;
    }

    @JsonGetter
    public Throwable err() {
      return err;
    }

    @JsonGetter
    public boolean status() {
      return status;
    }
  }