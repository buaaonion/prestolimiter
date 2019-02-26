package com.lc.prestolimiter.common;

//查询类型，用来控制某一类查询并发的量，当前只限制圈人.
public enum QueryType {
    NORMAL(-1),
    CIRCLE_PEOPLE(1);

    private int limit;

    QueryType(int limit) {
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }
}
