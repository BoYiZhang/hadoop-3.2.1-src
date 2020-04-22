package org.apache.hadoop.reflect;

import java.time.LocalDate;

public class Teacher implements People {
    @Override
    public String work() {
        System.out.println("上课=>"+ LocalDate.now() );
        return "上课";
    }
}
