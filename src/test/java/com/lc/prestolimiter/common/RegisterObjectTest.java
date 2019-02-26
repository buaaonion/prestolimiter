package com.lc.prestolimiter.common;


import com.alibaba.fastjson.JSON;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class RegisterObjectTest {

    @Test
    public void testSort() {
        List<RegisterObject> registerObjectList = new ArrayList<>();
        long time = System.currentTimeMillis();
        RegisterObject registerObject1 = new RegisterObject(1, QueryType.NORMAL, time + 1);
        RegisterObject registerObject2 = new RegisterObject(2, QueryType.NORMAL, time + 2);
        RegisterObject registerObject3 = new RegisterObject(1, QueryType.NORMAL, time + 3);
        RegisterObject registerObject4 = new RegisterObject(3, QueryType.NORMAL, time);
        RegisterObject registerObject5 = new RegisterObject(1, QueryType.NORMAL, time);
        registerObjectList.add(registerObject1);
        registerObjectList.add(registerObject2);
        registerObjectList.add(registerObject3);
        registerObjectList.add(registerObject4);
        registerObjectList.add(registerObject5);
        Collections.sort(registerObjectList);
        System.out.println(JSON.toJSONString(registerObjectList));
    }

    @Test
    public void test() {
        int num = 10000000;
        double fpp = 0.0001;
        long bits = ((long)((double)(-num) * Math.log(fpp) / (Math.log(2.0D) * Math.log(2.0D))));
        System.out.println(bits/(8*1024*1024));
        System.out.println(Math.max(1, (int)Math.round((double)(bits / num) * Math.log(2.0D))));
    }
}
