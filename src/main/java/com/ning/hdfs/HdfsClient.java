package com.ning.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * ClassName: HdfsClient
 * Description:
 * date: 2020/12/4 15:32
 *
 * @author ningjianjian
 */
public class HdfsClient {

    /**
     * 本地-》HDFS
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testPut() throws IOException, InterruptedException, URISyntaxException {

        String inputPath = "C:\\Users\\ningjianjian\\Desktop\\results.json";
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.171.102:9000"), configuration, "ning");
        fs.copyFromLocalFile(new Path(inputPath), new Path("/input/results.json"));

        fs.close();
    }

    /**
     * HFDS-》本地
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void testGet() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.171.102:9000"), configuration, "ning");

        // 2 查询
        fs.copyToLocalFile(new Path("/output"),new Path("C:\\Users\\ningjianjian\\Desktop\\op"));


        // 3 关闭资源
        fs.close();
    }


}
