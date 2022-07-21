package com.jbk.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {
    private FileSystem fs;
    @Before
public void init() throws URISyntaxException, IOException, InterruptedException {
    Configuration configuration = new Configuration();

    // FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration);
    fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration,"jbk");
}
    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        fs.mkdirs(new Path("/xiyou/huaguoshan/"));
    }
    @Test
    public void testPut() throws IOException {
        fs.copyFromLocalFile(false,false,new Path("G:\\大数据\\笔记\\随堂笔记.txt"),
                new Path("/xiyou/huaguoshan"));
    }
    @Test
    public void testGet() throws IOException {
        fs.copyToLocalFile(false,new Path("/xiyou/huaguoshan"),
                new Path("G:\\大数据\\笔记"),false);
    }
    @Test
    public void testRm() throws IOException {
        fs.delete(new Path("/xiyou/huaguoshan"),true);

    }
    @Test
    public void testmove() throws IOException {
        fs.rename(new Path("/input"),new Path("/output"));
        //fs.rename(new Path("/input/jbk.txt"),new Path("/"));
        //fs.rename(new Path("/input/word.txt"),new Path("/input/jbk.txt"));
    }
    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException {
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }
    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException{

        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

    }
    @After
    public void close() throws IOException {
        fs.close();
    }
}
