package org.apache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;

public class ClientTest {




    public ClientTest() throws IOException {
    }

    public static void main(String[] args) throws Exception {
        test01();
    }

    public static void test02() throws IOException {

        Configuration configuration = new Configuration();
        FileSystem fs  = FileSystem.get(configuration);

        FSDataOutputStream outputStream = fs.append(new Path("/README.txt"));

        for (int i=0 ; i<1000000 ; i++ ){

            String data= i + "正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正正" ;
            outputStream.write(data.getBytes(Charset.forName("UTF-8")));
            if(i % 100 == 0){
                outputStream.flush();
            }
        }


        outputStream.close();


    }


    public static void test01() throws IOException {

        Configuration configuration = new Configuration();
        FileSystem fs  = FileSystem.get(configuration);

        FSDataInputStream inputStream = fs.open(new Path("/README.txt"));

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String str = null;
        while((str = bufferedReader.readLine()) != null) {
            System.out.println(str);
        }

        //close
        inputStream.close();
        bufferedReader.close();
    }


}
