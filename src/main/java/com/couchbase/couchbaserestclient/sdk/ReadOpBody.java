package com.couchbase.couchbaserestclient.sdk;

import reactor.util.function.Tuple2;

import java.util.List;

public class ReadOpBody {
    public Server server;
    public List<String> keys;
    public CURDOptions curdOptions;

    public ReadOpBody(Server server, List<String> keys, CURDOptions curdOptions){
        this.server = server;
        this.keys = keys;
        this.curdOptions = curdOptions;
    }

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public CURDOptions getCurdOptions() {
        return curdOptions;
    }

    public void setCurdOptions(CURDOptions curdOptions) {
        this.curdOptions = curdOptions;
    }
}
