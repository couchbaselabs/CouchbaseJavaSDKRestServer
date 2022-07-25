package com.couchbase.couchbaserestclient.sdk;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonGetter;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class CURDOptions {
    public Integer timeout;
    public String time_unit;
    public String retryStrategy;
    public String durabilityLevel;
    public int persist_to;
    public int replicate_to;
    public Integer expiry;
    public String expiry_unit;

    @JsonCreator
    public CURDOptions(@JsonProperty("timeout") Integer timeout, @JsonProperty("time_unit") String time_unit,
                       @JsonProperty("retryStrategy") String retryStrategy, @JsonProperty("durabilityLevel") String durabilityLevel,
                       @JsonProperty("persist_to") int persist_to, @JsonProperty("replicate_to") int replicate_to,
                       @JsonProperty("expiry") Integer expiry, @JsonProperty("expiry_unit") String expiry_unit) {
        this.timeout = timeout;
        this.time_unit = time_unit;
        this.retryStrategy = retryStrategy;
        this.durabilityLevel = durabilityLevel;
        this.persist_to = persist_to;
        this.replicate_to = replicate_to;
        this.expiry = expiry;
        this.expiry_unit = expiry_unit;
    }

    @JsonCreator
    public CURDOptions(@JsonProperty("timeout") Integer timeout, @JsonProperty("time_unit") String time_unit,
                       @JsonProperty("retryStrategy") String retryStrategy, @JsonProperty("durabilityLevel") String durabilityLevel){
        this.timeout = timeout;
        this.time_unit = time_unit;
        this.retryStrategy = retryStrategy;
        this.durabilityLevel = durabilityLevel;
    }

    @JsonCreator
    public CURDOptions(@JsonProperty("timeout") Integer timeout, @JsonProperty("time_unit") String time_unit,
                       @JsonProperty("retryStrategy") String retryStrategy, @JsonProperty("durabilityLevel") String durabilityLevel,
                       @JsonProperty("expiry") Integer expiry, @JsonProperty("expiry_unit") String expiry_unit){
        this.timeout = timeout;
        this.time_unit = time_unit;
        this.retryStrategy = retryStrategy;
        this.durabilityLevel = durabilityLevel;
        this.expiry = expiry;
        this.expiry_unit = expiry_unit;
    }

    @JsonCreator
    public CURDOptions(@JsonProperty("timeout") Integer timeout, @JsonProperty("time_unit") String time_unit,
                       @JsonProperty("retryStrategy") String retryStrategy){
        this.timeout = timeout;
        this.time_unit = time_unit;
        this.retryStrategy = retryStrategy;
    }

    public CURDOptions(){}

    @JsonGetter
    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    @JsonGetter
    public String getTime_unit() {
        return time_unit;
    }

    public void setTime_unit(String time_unit) {
        this.time_unit = time_unit;
    }

    @JsonGetter
    public String getRetryStrategy() {
        return retryStrategy;
    }

    public void setRetryStrategy(String retryStrategy) {
        this.retryStrategy = retryStrategy;
    }

    @JsonGetter
    public String getDurabilityLevel() {
        return durabilityLevel;
    }

    public void setDurabilityLevel(String durabilityLevel) {
        this.durabilityLevel = durabilityLevel;
    }

    @JsonGetter
    public int getPersist_to() {
        return persist_to;
    }

    public void setPersist_to(int persist_to) {
        this.persist_to = persist_to;
    }

    @JsonGetter
    public int getReplicate_to() {
        return replicate_to;
    }

    public void setReplicate_to(int replicate_to) {
        this.replicate_to = replicate_to;
    }

    @JsonGetter
    public Integer getExpiry() {
        return expiry;
    }

    public void setExpiry(Integer expiry) {
        this.expiry = expiry;
    }

    @JsonGetter
    public String getExpiry_unit() {
        return expiry_unit;
    }

    public void setExpiry_unit(String expiry_unit) {
        this.expiry_unit = expiry_unit;
    }

    public Duration getDuration(Integer timeout, String time_unit) {
        ChronoUnit chrono_unit = ChronoUnit.MILLIS;
        time_unit = time_unit.toLowerCase();
        switch(time_unit) {
            case "seconds":
                chrono_unit = ChronoUnit.SECONDS;
                break;
            case "ms":
                chrono_unit = ChronoUnit.MILLIS;
                break;
            case "min":
                chrono_unit = ChronoUnit.MINUTES;
                break;
            case "hr":
                chrono_unit = ChronoUnit.HOURS;
                break;
            case "days":
                chrono_unit = ChronoUnit.DAYS;
                break;
        }
        return Duration.of(timeout, chrono_unit);
    }

    public RetryStrategy getRetryStrategy(String retryStrategy) {
        RetryStrategy _retryStrategy;
        _retryStrategy = BestEffortRetryStrategy.INSTANCE;
        if (retryStrategy != null && retryStrategy.toUpperCase().equals("FAIL_FAST"))
            _retryStrategy = FailFastRetryStrategy.INSTANCE;
        return _retryStrategy;
    }

    public DurabilityLevel getDurabilityLevel(String durabilityLevel) {
        DurabilityLevel durability;
        switch(durabilityLevel) {
            case "NONE":
                durability = DurabilityLevel.NONE;
                break;
            case "MAJORITY":
                durability = DurabilityLevel.MAJORITY;
                break;
            case "MAJORITY_AND_PERSIST_TO_ACTIVE":
                durability = DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
                break;
            case "PERSIST_TO_MAJORITY":
                durability = DurabilityLevel.PERSIST_TO_MAJORITY;
                break;
            default:
                durability = DurabilityLevel.NONE;
        }
        return durability;
    }

    public PersistTo getPersistTo(int persist_to) {
        PersistTo persistTo;
        switch(persist_to) {
            case 0:
                persistTo = PersistTo.NONE;
                break;
            case 1:
                persistTo = PersistTo.ACTIVE;
                break;
            case 2:
                persistTo = PersistTo.TWO;
                break;
            case 3:
                persistTo = PersistTo.THREE;
                break;
            case 4:
                persistTo = PersistTo.FOUR;
                break;
            default:
                persistTo = PersistTo.NONE;
        }
        return persistTo;
    }

    public ReplicateTo setReplicateTo(int replicate_to) {
        ReplicateTo replicateTo;
        switch(replicate_to) {
            case 0:
                replicateTo = ReplicateTo.NONE;
                break;
            case 1:
                replicateTo = ReplicateTo.ONE;
                break;
            case 2:
                replicateTo = ReplicateTo.TWO;
                break;
            case 3:
                replicateTo = ReplicateTo.THREE;
                break;
            default:
                replicateTo = ReplicateTo.NONE;
        }
        return replicateTo;
    }

    public InsertOptions getInsertOptions(){
        InsertOptions insertOptions = InsertOptions.insertOptions()
                .timeout(getDuration(this.timeout, this.time_unit))
                .durability(getDurabilityLevel(this.durabilityLevel))
                .retryStrategy(getRetryStrategy(this.retryStrategy));
        return  insertOptions;

    }

    public UpsertOptions getUpsertOptions(){
        UpsertOptions upsertOptions = UpsertOptions.upsertOptions()
                .timeout(getDuration(this.timeout, this.time_unit))
                .durability(getDurabilityLevel(this.durabilityLevel))
                .retryStrategy(getRetryStrategy(this.retryStrategy));
        return upsertOptions;
    }

    public UpsertOptions getExpiryOptions(){
        UpsertOptions upsertOptions = UpsertOptions.upsertOptions()
                .timeout(getDuration(this.timeout, this.time_unit))
                .durability(getDurabilityLevel(this.durabilityLevel))
                .retryStrategy(getRetryStrategy(this.retryStrategy))
                .expiry(getDuration(this.expiry, this.expiry_unit));
        return upsertOptions;
    }

    public RemoveOptions getRemoveOptions(){
        RemoveOptions removeOptions = RemoveOptions.removeOptions()
                .timeout(getDuration(this.timeout, this.time_unit))
                .durability(getDurabilityLevel(this.durabilityLevel))
                .retryStrategy(getRetryStrategy(this.retryStrategy));
        return removeOptions;
    }

    public GetOptions getGetOptions(){
        GetOptions getOptions = GetOptions.getOptions()
                .timeout(getDuration(this.timeout, this.time_unit))
                .retryStrategy(getRetryStrategy(this.retryStrategy));
        return getOptions;
    }

    public ReplaceOptions getReplaceOptions(){
        ReplaceOptions replaceOptions = ReplaceOptions.replaceOptions()
                .timeout(getDuration(this.timeout, this.time_unit))
                .durability(getDurabilityLevel(this.durabilityLevel))
                .retryStrategy(getRetryStrategy(this.retryStrategy));
        return replaceOptions;
    }

    public TouchOptions getTouchOptions(){
        TouchOptions touchOptions = TouchOptions.touchOptions()
                .timeout(getDuration(this.timeout, this.time_unit))
                .retryStrategy(getRetryStrategy(this.retryStrategy));
        return touchOptions;
    }
}
