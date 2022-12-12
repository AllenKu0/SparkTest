package org.example;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple1;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Main {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("appName").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String outputPath = "/Users/imac-1682/Documents/test2";
//        FileUtils.deleteDirectory(new File(outputPath));
//        SparkConf conf = new SparkConf(true);
//        conf.setMaster("local")
//                .setAppName("RefineOnSpark_local")
//                .set("spark.hadoop.fs.local.block.size",
//                        new Long(16 * 1024 * 1024).toString());
//        JavaSparkContext sc = new JavaSparkContext(conf);

//        System.out.println("Hello world!");

//        Tuple1<String> tuple1 = new Tuple1<>("aaa");

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        String result = distData
                .map(s -> s+"123")
                .reduce((a,b) -> a+","+b);

        System.out.println(result);

//        distData.flatMap()
        int sum;
        sum = distData.reduce((a, b) -> a + b);
        System.out.println(sum);
        //讀檔
//        String path = "/Users/imac-1682/Documents/test.txt";
        String path = "/Users/imac-1682/IdeaProjects/RefineOnSpark/RefineOnSpark/target/test-classes/input/50.csv";
        JavaRDD<String> distFile = sc.textFile(path);
//        int ans;
//        ans = distFile.map(s -> {
//            s = s+"123";
//            return s.length();
//        }).reduce((a, b) -> {
//            System.out.println(a);
//            System.out.println(b+";");
//           return a + b;
//        });
//        System.out.println(ans);

        //distFile.first() distFile的第一行
        //將資料傳送至每個節點，使每個節點資料相同，也只要創一個
        Broadcast<String> header = sc.broadcast(distFile.first());
        //獲取Broadcast的值
        System.err.println(header.getValue());
        JavaRDD<String> export = distFile
                .map(s -> s+"123");
        //會輸出至一資料夾
        export.saveAsTextFile(outputPath);
        //

//        JavaRDD<String> rdd = sc.parallelize(
//                new ArrayList<>(List.of("coffee panda","happy panda","happiest panda party")));

//        rdd.map(s -> s.split(" ")).collect().forEach(
//                strings -> System.out.println(strings));
//
//        rdd.flatMap(s -> s.split(" ")).collect().forEach(
//                strings -> System.out.println(strings));
    }

//    public void submitJob(String path,int partition) {
//        SparkConf conf = new SparkConf(true);
//        conf.setMaster("local")
//                .setAppName("RefineOnSpark_local")
//                .set("spark.hadoop.fs.local.block.size",
//                        new Long(16 * 1024 * 1024).toString());
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> rdd = sc.textFile(path,partition);
//
//        int ans;
//
//        ans = rdd.map(s -> {
//            System.out.println(s);
//            return s.length();
//        }).reduce((a, b) -> {
//            System.out.println(a);
//            System.out.println(b+";");
//            return a + b;
//        });
//    }

//    public void close() {
//        if()
//    }
}