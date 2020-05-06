package org.apache;


import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Test;

public class NameNodeTest {


    @Test
    public void testNamenode() throws Exception {

       NameNode.main(new String[]{});
    }


    @Test
    public void testSize() throws Exception {
        long size = Integer.MAX_VALUE ;

        System.out.println(size);
    }

    @Test
    public void testFormat() throws Exception {

        System.out.println(String.format("%s_%019d", "image", 123131));

    }




    //compute actual length
    @Test
    public  void actualArrayLength() {
        int recommended= 3 ;


        int MAX_ARRAY_LENGTH = 1 << 30; //prevent int overflow problem
        int MIN_ARRAY_LENGTH = 1;


        if (recommended > MAX_ARRAY_LENGTH) {
            System.out.println(MAX_ARRAY_LENGTH);
        } else if (recommended < MIN_ARRAY_LENGTH) {
            System.out.println(MIN_ARRAY_LENGTH);
        } else {
            int a = Integer.highestOneBit(recommended);
            System.out.println(a);
            System.out.println(recommended);
            System.out.println("result : "  +  ( (a == recommended) ? a: a << 1 ) );
            System.out.println("-------------------------------");
        }
    }




}
