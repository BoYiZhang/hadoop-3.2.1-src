package org.apache;

import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Test;

public class DataNodeTest {

    @Test
    public void testDatanode() throws Exception {

        DataNode.main(new String[]{});
    }
}
