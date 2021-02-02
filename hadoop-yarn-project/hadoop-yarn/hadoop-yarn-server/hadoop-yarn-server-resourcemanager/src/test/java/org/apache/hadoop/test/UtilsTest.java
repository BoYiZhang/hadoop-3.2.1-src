package org.apache.hadoop.test;

import org.apache.commons.lang3.RandomUtils;

public class UtilsTest {

    public static void main(String[] args) {

        long totalAvailable = 0L;
        final long[] availableOnDisk = new long[] {10,20,30,40,50,60,70,80,10,100} ;
        for (long c: availableOnDisk ) {
            totalAvailable += c ;
        }

        int num = 0 ;
        while (num < 10 ) {
            System.out.println("====================================");
            long randomPosition = RandomUtils.nextLong() % totalAvailable;
            int index = pickDirectory(randomPosition, availableOnDisk) ;
            System.out.println("num : "+num+" ,randomPosition : "+randomPosition+" ,  index : "+ index);
            num ++ ;
        }


    }


    public static int pickDirectory(long randomPosition, final long[] availableOnDisk) {
        int dir = 0;
        // 跳过零可用空间目录，
        // 因为totalAvailable大于0，而randomPosition小于totalAvailable
        // 我们可以找到可用空间非零的有效目录。

        // skip zero available space directory,
        // because totalAvailable is greater than 0 and randomPosition
        // is less than totalAvailable, we can find a valid directory
        // with nonzero available space.
        while (availableOnDisk[dir] == 0L) {
            dir++;
        }


        long availableOnDiskVaule  = availableOnDisk[dir];


        while (randomPosition >= availableOnDiskVaule ) {
            System.out.println("**************************************************");
            System.out.println("now-randomPosition:"+randomPosition);
            System.out.println("now-availableOnDiskVaule:"+availableOnDiskVaule);
            randomPosition -= availableOnDisk[dir++];
            System.out.println("next-randomPosition:"+randomPosition);
            System.out.println("next-availableOnDisk:"+availableOnDisk[dir]);
        }
        return dir;
    }


}
