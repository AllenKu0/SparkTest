package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import org.example.client.RemoteInterface;
import org.example.utils.DriverCLIOptions;
import org.example.utils.StringAccumulatorParam;
import org.kohsuke.args4j.CmdLineParser;
import scala.Tuple1;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Main implements RemoteInterface {
    private static JavaSparkContext sparkContext;

    private static Main remoteObj;

    public static RemoteInterface stub;

    public Main() {
        SparkConf conf = new SparkConf(true);
        conf.setMaster("local")
                .setAppName("RefineOnSpark_local")
                .set("spark.hadoop.fs.local.block.size",
                        new Long(16 * 1024 * 1024).toString());
        sparkContext = new JavaSparkContext(conf);
    }

    public Main(SparkConf conf) {
        sparkContext = new JavaSparkContext(conf);
        System.out.println("SparkContext initialized, connected to master: "
                + sparkContext.master());
    }

    ;

    public static void main(String[] args) throws IOException {
        //local表示建在本地，單機模式、cluster為多機模式
//        SparkConf conf = new SparkConf().setAppName("appName").setMaster("local");
        ServerSocket stubServer = null;
        ObjectOutputStream objStream = null;

        DriverCLIOptions cLineOptions = new DriverCLIOptions();
//        cLineOptions.setDriverHost("10.20.1.13");
//        cLineOptions.setDriverPort("7077");
        cLineOptions.getArguments().add("spark://10.20.1.13:7077");
        CmdLineParser parser = new CmdLineParser(cLineOptions);

        try {
            parser.parseArgument(args);

            if (cLineOptions.getArguments().size() < 1) {
                printUsage(parser);
                System.err.println("哭啊");
                //0為正常退出程式，非0則為不正常退出
//				System.exit(-1);
            }

            remoteObj = new Main(configureSpark(cLineOptions));
            stub = (RemoteInterface) UnicastRemoteObject.exportObject(
                    remoteObj, 0);
            String outputPath = "/Users/imac-1682/Documents/test2";
            String path = "/Users/imac-1682/IdeaProjects/RefineOnSpark/RefineOnSpark/target/test-classes/input/50.csv";
            String transformFile = "/Users/imac-1682/IdeaProjects/SparkTest/target/operations";

            JavaRDD<String> distFile = sparkContext.textFile(path);
            Broadcast<String> header = sparkContext.broadcast(distFile.first());
            distFile = distFile.mapPartitions(new JobFunction(header, transformFile,
                    new StringAccumulatorParam()));

            distFile.saveAsTextFile(outputPath);
//        JavaSparkContext sc = new JavaSparkContext(conf);
//            while (true) {
//                try {
//                    //建立服務端
//                    stubServer = new ServerSocket(3377);
////					stubServer = new ServerSocket(4040);
//                    System.out.println("Waiting for connections");
//                    System.out.println("1");
//
//                    //接受連接，當連接時可以透過connection操控練過來的客服端
//                    Socket connection = stubServer.accept();
//                    System.out.println("2");
//
//
//                    System.out.println("Connection accepted! From: "
//                            + connection.getRemoteSocketAddress().toString()
//                            + " Sending stub!");
//                    //獲得客服端的輸出流
//                    objStream = new ObjectOutputStream(
//                            connection.getOutputStream());
//                    //連線時，輸出我遠程的客服端為何
//                    objStream.writeObject(stub);
//                    objStream.close();
//
//                } catch (Exception e) {
//                    System.out.println("Failed accepting request: "
//                            + e.toString());
//                    e.printStackTrace();
//                } finally {
//                    System.out.println("3");
//                    IOUtils.closeQuietly(stubServer);
//                }
//            }

//		} catch (CmdLineException e) {
//			printUsage(parser);
//			System.exit(-1);
//		}
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (sparkContext != null)
                sparkContext.stop();
        }

    }

//    public void close() {
//        if()
//    }

    private static SparkConf configureSpark(DriverCLIOptions cLineOptions) {

//        String currentWorkDir = System.getProperty("user.dir");
        System.out.println(cLineOptions.getArguments().get(0));
        System.out.println(cLineOptions.getAppName());
        SparkConf sparkConfiguration = new SparkConf(true)
                .setMaster(cLineOptions.getArguments().get(0))
                .setAppName(cLineOptions.getAppName())
                .set("spark.executor.memory", cLineOptions.getExecutorMemory());
//                .set("spark.driver.host",cLineOptions.getDriverHost())
//                .set("spark.driver.port",cLineOptions.getDriverPort());
//                .set("spark.executor.extraClassPath",
//                        currentWorkDir + "/RefineOnSpark-0.1.jar:"
//                                + currentWorkDir + "/lib/*")
//                .set("spark.hadoop.fs.local.block.size",
//                        cLineOptions.getFsBlockSize().toString());
        return sparkConfiguration;
    }

    private static void printUsage(CmdLineParser parser) {
        System.err
                .println("Usage: refineonspark [OPTION...] SPARK://MASTER:PORT\n");
        parser.printUsage(System.err);
    }

    @Override
    public String submitJob(String[] args) throws RemoteException, Exception {
//        SparkConf conf = new SparkConf(true);
//        conf.setMaster("local")
//                .setAppName("RefineOnSpark_local")
//                .set("spark.hadoop.fs.local.block.size",
//                        new Long(16 * 1024 * 1024).toString());
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> rdd = sc.textFile(path,partition);

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
//        return String.format("%18s\t%2.3f%s",
//                FilenameUtils.getName(options[0]),
//                transFormTime / 1000000000.0, accum.value());
        return null;
    }
}