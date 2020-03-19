package org.apache.hadoop.tmp;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class test {

    private static final ThreadLocal<Integer> callId = new ThreadLocal<Integer>();
    /** A counter for generating call IDs. */
    private static final AtomicInteger callIdCounter = new AtomicInteger();


    public static void main(String[] args) {
        // 21 4748 3647
        Integer data = (1 << 31) - 1 ;
//        System.out.println( data );
//        System.out.println( Integer.MAX_VALUE );
        System.out.println( 21_4748_3647 & 0x7fffffff );


//        System.out.println( data );
//        for (int i = 0 ;  i < 100; i++ ) {
//
//            System.out.println(nextCallId());
//        }

    }
    public static int nextCallId() {
        return callIdCounter.getAndIncrement() & 0x7FFFFFFF;
    }

    public void testMap(){
        Map<Integer,String> map = new HashMap<>();
        map.put(1,"a");
        map.put(2,"b");
        map.put(3,"c");

        for (Integer i : map.keySet() ) {
            System.out.println(map.get(i));
        }

        System.out.println("=============================");
        map.putIfAbsent(1,"aaaaa");
        map.put(2,"bbbbb");

        for (Integer i : map.keySet() ) {
            System.out.println(map.get(i));
        }

    }

}
