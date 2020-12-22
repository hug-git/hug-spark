package com.atguigu.es.app;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsReaderByJava {
    public static void main(String[] args) throws IOException {
        JestClientFactory jestClientFactory = new JestClientFactory();
        jestClientFactory.setHttpClientConfig(
                new HttpClientConfig.Builder("http://hadoop102:9200").build()
        );
        JestClient jestClient = jestClientFactory.getObject();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("class_id", "shuhan");
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().filter(termQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);

        System.out.println("=-=-=-=-=\n"+ searchSourceBuilder.toString()+"=-=-=-=-=\n");
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("stu")
                .addType("_doc")
                .build();

        SearchResult result = jestClient.execute(search);
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("================================");
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println(o + ":" + source.get(o));
            }
        }

        jestClient.shutdownClient();
    }
}
