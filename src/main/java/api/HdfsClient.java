package api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    @Test
    public void testMkdirs() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
        fs.mkdirs(new Path("/test/mkdirs"));
        fs.close();
    }

    @Test
    public void testCopyFromLocalFile() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
        fs.copyFromLocalFile(new Path("g:/log4j.properties"), new Path("/test/mkdirs"));
        fs.close();
    }

    @Test
    public void testCopyToLocalFile() throws Exception{
        // 1 获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/test/mkdirs/log4j.properties"), new Path("g:/log4j.properties"), true);
        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
        // 2 执行删除
        fs.delete(new Path("/test/mkdirs/"), true);
        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
        // 2 修改文件名称
        fs.rename(new Path("/test/mkdirs/log4j.properties2"), new Path("/test/mkdirs/log4j.properties"));
        // 3 关闭资源
        fs.close();
    }

}
