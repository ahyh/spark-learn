package com.yh.java.mkdata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 用于造数据的类
 *
 * @author yanhuan
 */
public class MakeAccessLog {

    public static void main(String[] args) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(new File("D:\\spark\\teacher.log")));
        List<String> collect = reader.lines().collect(Collectors.toList());
        FileWriter writer = new FileWriter(new File("D:\\spark\\1.log"));
        for (int i = 0; i < 20000; i++) {
            for (String s : collect) {
                writer.write(s + "\r\n");
            }
        }
        writer.close();
        reader.close();
    }
}
