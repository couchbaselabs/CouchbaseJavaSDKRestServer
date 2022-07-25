package com.couchbase.couchbaserestclient.sdk;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class WriteOpBody {
    public Server server;
    public ArrayList<HashMap<String, Object>> documents;
    //public ArrayList<Tuple2<String, Object>> documents;
    public CURDOptions curdOptions;

    public WriteOpBody(Server server, ArrayList<HashMap<String, Object>> documents, CURDOptions curdOptions){
        this.server = server;
        this.documents = documents;
        this.curdOptions = curdOptions;
    }

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    public List<Tuple2<String, Object>> getDocuments() {
        List<Tuple2<String, Object>> docs = new ArrayList<Tuple2<String,Object>>();
        for(HashMap<String, Object> document:this.documents){
            String key;
            Object val;
            key = (String) document.get("t1");
            val = document.get("t2");
            docs.add(Tuples.of(key, val));
        }
        return docs;
    }

    public void setDocuments(ArrayList<HashMap<String, Object>> documents) {
        this.documents = documents;
    }

    public CURDOptions getCurdOptions() {
        return curdOptions;
    }

    public void setCurdOptions(CURDOptions curdOptions) {
        this.curdOptions = curdOptions;
    }
}
