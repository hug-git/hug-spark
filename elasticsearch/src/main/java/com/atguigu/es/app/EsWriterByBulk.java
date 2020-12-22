package com.atguigu.es.app;

import com.atguigu.es.bean.Stu;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriterByBulk {
    public static void main(String[] args) throws IOException {
        JestClientFactory jestClientFactory = new JestClientFactory();
        jestClientFactory.setHttpClientConfig(
                new HttpClientConfig.Builder("http://hadoop102:9200").build()
        );
        JestClient jestClient = jestClientFactory.getObject();

        Stu stu1 = new Stu("shuhan", "诸葛亮", "male", 26, "鞠躬尽瘁死而后已", "鞠躬尽瘁死而后已");
        Index index1 = new Index.Builder(stu1)
                .id("1012")
                .build();

//        Stu stu2 = new Stu("shuhan", "庞统", "male", 28, "视曹操孙权如掌中玩物", "视曹操孙权如掌中玩物");
//        Index index2 = new Index.Builder(stu2)
//                .id("1013")
//                .build();

        Bulk bulk = new Bulk.Builder()
                .addAction(index1)
//                .addAction(index2)
                .defaultIndex("stu")
                .defaultType("_doc")
                .build();

        jestClient.execute(bulk);

        jestClient.shutdownClient();

    }
}
