package com.auguuuste.study.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {


    Configuration configuration = new Configuration();

    FileSystem fs = null;

    private static final String URI = "hdfs://hadoop101:9000";

    @Before
    public void setup() throws URISyntaxException, IOException, InterruptedException {
        System.setProperty("hadoop.home.dir","C:\\Program Files\\Java\\hadoop-2.7.2" );
        // 1 获取文件系统
        configuration = new Configuration();
        // 配置在集群上运行
        fs = FileSystem.get(new URI(URI), configuration, "root");
    }

    @After
    public void close() throws IOException {
        fs.close();
        System.out.println("—————————————END—————————————");
    }

    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {
        fs.mkdirs(new Path("/1108/daxian/banzhang"));
    }

    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
        fs.copyFromLocalFile(new Path("D:\\code\\study-hadoop\\pom.xml"), new Path("/pom.xml"));
    }

    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{
        fs.copyToLocalFile(false, new Path("/pom.xml"), new Path("D:\\POM"), true);
    }

    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException{
        fs.delete(new Path("/1108/"), true);
    }

    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException{
        fs.rename(new Path("/pom.xml"), new Path("/POM"));
    }

    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
    }

    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("d:/ant-optional-1.5.3-1.pom"));
        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/ant-optional-1.5.3-1.pom"));
        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

    // 文件下载
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{
        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/ant-optional-1.5.3-1.pom"));
        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("d:/banhua.txt"));
        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

}
