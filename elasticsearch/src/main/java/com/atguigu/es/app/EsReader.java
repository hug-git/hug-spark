package com.atguigu.es.app;

import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsReader {
    public static void main(String[] args) throws IOException {
        // 创建客户端对象
        JestClientFactory jestClientFactory = new JestClientFactory();
        jestClientFactory.setHttpClientConfig(
                new HttpClientConfig.Builder("http://hadoop102:9200").build()
        );
        JestClient jestClient = jestClientFactory.getObject();

        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"favo1\": \"桃园\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}")
                .addIndex("stu")
                .addType("_doc")
                .build();

        SearchResult result = jestClient.execute(search);
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("================================");
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println(o + ": " + source.get(o));
            }
        }


    }
}
