package com.atguigu.es.app;

import com.atguigu.es.bean.Stu;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriter {
    public static void main(String[] args) throws IOException {
        // 1.创建客户端对象
        // 1.1创建工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();
        // 1.2设置连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig
                .Builder("http://hadoop102:9200")
                .build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        // 1.3获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        // 2.操作数据
        // 2.1创建Index对象
        Index index = new Index.Builder("{\n" +
                "  \"class_id\":\"wei\",\n" +
                "  \"name\":\"曹操\",\n" +
                "  \"gender\":\"male\",\n" +
                "  \"age\":35,\n" +
                "  \"favo1\":\"我爱死他了\",\n" +
                "  \"favo2\":\"我爱死他了\"\n" +
                "}")
                .index("stu")
                .type("_doc")
                .id("1010")
                .build();

        Stu stu = new Stu("wei","杨修","male",35,"我知主公意","我知主公意");
        Index index1 = new Index.Builder(stu)
                .index("stu")
                .type("_doc")
                .id("1011")
                .build();

        jestClient.execute(index1);

        // 3.关闭连接
        jestClient.shutdownClient();

    }
}
