package org.example;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("appName").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println("Hello world!");

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        int sum;
        sum = distData.reduce((a, b) -> a + b);
        System.out.println(sum);
        //讀檔
        String path = "/Users/imac-1682/Documents/test.txt";
        JavaRDD<String> distFile = sc.textFile(path);
        int ans;
        ans = distFile.map(s -> s.length()).reduce((a, b) -> a + b);
        System.out.println(ans);
    }
}