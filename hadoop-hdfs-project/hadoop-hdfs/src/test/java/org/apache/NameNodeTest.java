package org.apache;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.BeforeClass;
import org.junit.Test;

public class NameNodeTest {

    @Test
    public void testNamenode() throws Exception {

       NameNode.main(new String[]{});
    }


    @Test
    public void testSize() throws Exception {
        long size = 1 << 48 ;

        System.out.println(size);
    }

    @Test
    public void testFormat() throws Exception {

        System.out.println(String.format("%s_%019d", "image", 123131));

    }



}
