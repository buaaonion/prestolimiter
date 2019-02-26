package com.lc.prestolimiter.common;

public class RegisterObject implements Comparable<RegisterObject> {

    private int priority;
    private long expireTime;
    private QueryType queryType;
    private String node;

    public RegisterObject() {
    }

    public RegisterObject(int priority, long expireTime, QueryType queryType, String node) {
        this.priority = priority;
        this.expireTime = expireTime;
        this.queryType = queryType;
        this.node = node;
    }

    public RegisterObject(int priority, QueryType queryType, long expireTime) {
        this.priority = priority;
        this.expireTime = expireTime;
        this.queryType = queryType;
    }

    public RegisterObject(RegisterObject registerObject) {
        this.priority = registerObject.priority;
        this.expireTime = registerObject.expireTime;
        this.queryType = registerObject.queryType;
        this.node = registerObject.node;
    }

    public int getPriority() {
        return priority;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public String getNode() {
        return node;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    @Override
    public int hashCode() {
        int hashCode = this.priority;
        hashCode = (int) (hashCode * 31 + this.expireTime);
        hashCode = hashCode * 31 + this.queryType.hashCode();
        if (this.node != null) {
            hashCode = hashCode * 31 + this.node.hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RegisterObject) {
            return this.expireTime == ((RegisterObject) obj).expireTime
                && this.priority == ((RegisterObject) obj).priority
                && this.queryType == ((RegisterObject) obj).queryType
                && ((this.node != null && this.node.equals(((RegisterObject) obj).node))
                || (this.node == null && ((RegisterObject) obj).node == null));
        }
        return false;
    }

    @Override
    public int compareTo(RegisterObject registerObject) {
        if (this.priority == registerObject.priority) {
            return this.expireTime >= registerObject.expireTime ? 1 : -1;
        } else {
            return registerObject.priority - this.priority;
        }
    }
}
