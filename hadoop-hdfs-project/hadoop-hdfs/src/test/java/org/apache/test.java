package org.apache;

import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;

import java.io.File;

public class test {
    public static void main(String[] args) {

        File file = DatanodeUtil.idToBlockDir(new File("/tools/hadoop-3.2.1/data/hdfs/data/current/BP-451827885-192.168.8.156-1584099133244/current/finalized"),2073741825L);

        System.out.println(file.getPath());

    }
}