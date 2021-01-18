package org.apache.hadoop.test;

import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class NodeManagerTest {

    @Test
    public void testNodeManager() throws Exception {
        NodeManager.main(new String[]{});


        TimeUnit.SECONDS.sleep(100000L);
    }



}