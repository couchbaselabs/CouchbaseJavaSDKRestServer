package com.couchbase.couchbaserestclient.RestService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.couchbaserestclient.sdk.*;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.util.function.Tuple2;

@RestController
public class CouchbaseController {

    private Map<String, SDKClient> sdkClients = new HashMap<String, SDKClient>();
    private DocOps docOps = new DocOps();

    @PostMapping("/connectServer")
    public boolean connectCluster(@RequestBody Server server){
        if (sdkClients.containsKey(server.ip)){
            return true;
        }
        try {
            SDKClient sdkClient = new SDKClient(server);
            sdkClient.connectCluster();
            sdkClients.put(server.ip, sdkClient);
            return true;
        }
        catch (Exception e){
            return false;
        }
    }

    @PostMapping("/initialiseSDK/{bucket}/{scope}/{collection}")
    public boolean initialiseSDK(@RequestBody Server server, @PathVariable String bucket, @PathVariable String scope, @PathVariable String collection){
        String key = server.ip + "_" + bucket + "_" + scope + "_" + collection;
        if(sdkClients.containsKey(key)){
            return true;
        }
        SDKClient sdkClient = new SDKClient(server, bucket, scope, collection);
        try {
            sdkClient.initialiseSDK();
            sdkClients.put(key, sdkClient);
            return true;
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    @PostMapping("/connectBucket/{bucket}")
    public Bucket connectBucket(@RequestBody Server server, @RequestBody String bucket){
        SDKClient sdkClient = this.getSDKClient(server);
        return sdkClient.connectBucket(bucket);
    }

    @PostMapping("/selectCollection/{bucket}/{scope}/{collection}")
    public Collection selectCollection(@RequestBody Server server, @PathVariable String bucket, @PathVariable String scope, @PathVariable String collection){
        String key = this.constructKey(server, bucket, scope, collection);
        if(sdkClients.containsKey(key)){
            SDKClient sdkClient = sdkClients.get(key);
            return sdkClient.connection;
        }
        SDKClient sdkClient = this.getSDKClient(server);
        Bucket bucketObj = sdkClient.getBucketObj();
        if(bucketObj==null || bucketObj.name() != bucket){
            bucketObj = sdkClient.connectBucket(bucket);
        }
        return sdkClient.selectCollection(scope, collection);
    }

    @PostMapping("/disconnectCluster")
    public boolean disconnectCluster(@RequestBody Server server){
        if(sdkClients.containsKey(server.ip)){
            SDKClient sdkClient = sdkClients.get(server.ip);
            sdkClient.disconnectCluster();
            return true;
        }
        return false;
    }

    @PostMapping("/bulkInsert/{bucket}/{scope}/{collection}")
    public List<Result> bulkInsert(@RequestBody WriteOpBody writeOpBody, @PathVariable String bucket, @PathVariable String scope, @PathVariable String collection){
        Server server = writeOpBody.getServer();
        List<Tuple2<String, Object>> documents = writeOpBody.getDocuments();
        CURDOptions curdOptions = writeOpBody.getCurdOptions();
        String key = this.constructKey(server, bucket, scope, collection);
        if(!sdkClients.containsKey(key)){
            initialiseSDK(server, bucket, scope, collection);
        }
        Collection connection = sdkClients.get(key).connection;
        return this.docOps.bulkUpsert(connection, documents, curdOptions.getUpsertOptions());
    }

    @PostMapping("/bulkUpsert/{bucket}/{scope}/{collection}")
    public List<Result> bulkUpsert(@RequestBody WriteOpBody writeOpBody, @PathVariable String bucket, @PathVariable String scope, @PathVariable String collection){
        Server server = writeOpBody.getServer();
        List<Tuple2<String, Object>> documents = writeOpBody.getDocuments();
        CURDOptions curdOptions = writeOpBody.getCurdOptions();
        String key = this.constructKey(server, bucket, scope, collection);
        if(!sdkClients.containsKey(key)){
            initialiseSDK(server, bucket, scope, collection);
        }
        Collection connection = sdkClients.get(key).connection;
        return this.docOps.bulkInsert(connection, documents, curdOptions.getInsertOptions());
    }

    @PostMapping("/bulkGet/{bucket}/{scope}/{collection}")
    public List<Tuple2<String, Object>> bulkGet(@RequestBody WriteOpBody writeOpBody, @PathVariable String bucket, @PathVariable String scope, @PathVariable String collection){
        Server server = writeOpBody.getServer();
        List<Tuple2<String, Object>> documents = writeOpBody.getDocuments();
        CURDOptions curdOptions = writeOpBody.getCurdOptions();
        String key = this.constructKey(server, bucket, scope, collection);
        if(!sdkClients.containsKey(key)){
            initialiseSDK(server, bucket, scope, collection);
        }
        Collection connection = sdkClients.get(key).connection;
        return this.docOps.bulkGets(connection, documents, curdOptions.getGetOptions());
    }

    @PostMapping("/bulkDelete/{bucket}/{scope}/{collection}")
    public List<Result> bulkDelete(@RequestBody ReadOpBody readOpBody, @PathVariable String bucket, @PathVariable String scope, @PathVariable String collection){
        Server server = readOpBody.getServer();
        List<String > keys = readOpBody.getKeys();
        CURDOptions curdOptions = readOpBody.getCurdOptions();
        String key = this.constructKey(server, bucket, scope, collection);
        if(!sdkClients.containsKey(key)){
            initialiseSDK(server, bucket, scope, collection);
        }
        Collection connection = sdkClients.get(key).connection;
        return this.docOps.bulkDelete(connection, keys, curdOptions.getRemoveOptions());
    }

    @PostMapping("/bulkReplace/{bucket}/{scope}/{collection}")
    public List<ConcurrentHashMap<String, Object>> bulkReplace(@RequestBody WriteOpBody writeOpBody, @PathVariable String bucket, @PathVariable String scope, @PathVariable String collection) {
        Server server = writeOpBody.getServer();
        List<Tuple2<String, Object>> documents = writeOpBody.getDocuments();
        CURDOptions curdOptions = writeOpBody.getCurdOptions();
        String key = this.constructKey(server, bucket, scope, collection);
        if(!sdkClients.containsKey(key)){
            initialiseSDK(server, bucket, scope, collection);
        }
        Collection connection = sdkClients.get(key).connection;
        return this.docOps.bulkReplace(connection, documents, curdOptions.getReplaceOptions());
    }

    @PostMapping("/bulkTouch/{bucket}/{scope}/{collection}")
    public List<ConcurrentHashMap<String, Object>> bulkTouch(@RequestBody ReadOpBody readOpBody, @PathVariable String bucket, @PathVariable String scope, @PathVariable String collection){
        Server server = readOpBody.getServer();
        List<String > keys = readOpBody.getKeys();
        CURDOptions curdOptions = readOpBody.getCurdOptions();
        String key = this.constructKey(server, bucket, scope, collection);
        if(!sdkClients.containsKey(key)){
            initialiseSDK(server, bucket, scope, collection);
        }
        Collection connection = sdkClients.get(key).connection;
        return this.docOps.bulkTouch(connection, keys, curdOptions.getTouchOptions(),
                curdOptions.getDuration(curdOptions.expiry, curdOptions.expiry_unit));
    }

    private SDKClient getSDKClient(Server server){
        SDKClient sdkClient;
        if (sdkClients.containsKey(server.ip)){
            sdkClient = sdkClients.get(server.ip);
        }
        else {
            sdkClient = new SDKClient(server);
            sdkClient.connectCluster();
            sdkClients.put(server.ip, sdkClient);
        }
        return sdkClient;
    }

    private String constructKey(Server server, String bucket, String scope, String collection){
        return server.ip + "_" + bucket + "_" + scope + "_" + collection;
    }
}
