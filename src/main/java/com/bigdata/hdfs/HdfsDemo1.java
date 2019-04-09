package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsDemo1 {

    /**
     * 测试获取客户端，然后查看hdfs上的内容
     */
    @Test
    public void getClient() throws IOException, URISyntaxException, InterruptedException {
        Configuration entries = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), entries, "yanglei");


        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fs1:fileStatuses) {
//            fs1.isFile();判断是否是文件
            System.out.println(fs1.getPath());
        }

//        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
//
//        while (listFiles.hasNext()) {
//            LocatedFileStatus status = listFiles.next();
//            // 输出详情
//            // 文件名称
//            System.out.println(status.getPath());
//                 // 长度
//                System.out.println(status.getLen());
//                // 权限
//                System.out.println(status.getPermission());
//                // z组
//                System.out.println(status.getGroup());
//
//                // 获取存储的块信息
//                BlockLocation[] blockLocations = status.getBlockLocations();
//
//                for (BlockLocation blockLocation : blockLocations) {
//
//                    // 获取块存储的主机节点
//                    String[] hosts = blockLocation.getHosts();
//
//                    for (String host : hosts) {
//                        System.out.println(host);
//                    }
//                }

//        }
    }

    /**
     * 测试获取客户端，创建文件夹
     */
    @Test
    public void mkdirs() throws IOException, URISyntaxException, InterruptedException {
        Configuration entries = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), entries, "yanglei");

        boolean mkdirs = fs.mkdirs(new Path("/user/yanglei/123/"));
        System.out.println("创建:" + mkdirs);
        fs.close();
    }

    /**
     * 测试获取客户端，上传文件
     */
    @Test
    public void upload() throws IOException, URISyntaxException, InterruptedException {
        Configuration entries = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), entries, "yanglei");

        //上传,
        fs.copyFromLocalFile(false, new Path("C:\\Users\\admin\\Desktop\\11.txt"), new Path("/user/yanglei"));


        //下载
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
//        fs.copyToLocalFile(false, new Path("/hello1.txt"), new Path("e:/hello1.txt"), true);


        //删除
//        fs.delete(new Path("/test/"), true);

    }

    /**
     * 通过io进行上传
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "yanglei");

        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("e:/hello.txt"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/hello4.txt"));

        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }


}
