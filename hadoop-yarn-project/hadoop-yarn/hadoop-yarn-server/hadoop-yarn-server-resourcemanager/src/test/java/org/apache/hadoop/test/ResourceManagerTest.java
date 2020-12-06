package org.apache.hadoop.test;

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class ResourceManagerTest {

    @Test
    public void testResourceManager() throws Exception {
        ResourceManager.main(new String[]{});


        TimeUnit.SECONDS.sleep(100000L);
    }



}