package org.apache;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.BeforeClass;
import org.junit.Test;

public class NameNodeTest {

    @Test
    public void testNamenode() throws Exception {
       NameNode.main(new String[]{});
    }
}
