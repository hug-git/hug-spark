package com.atguigu.es.app;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsReaderCase {
    public static void main(String[] args) throws IOException {
        JestClientFactory jestClientFactory = new JestClientFactory();
        jestClientFactory.setHttpClientConfig(
                new HttpClientConfig.Builder("http://hadoop102:9200").build()
        );
        JestClient jestClient = jestClientFactory.getObject();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 查询
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("class_id", "shuhan");
        boolQueryBuilder.filter(termQueryBuilder);
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo1", "捅他一万个透明窟窿");
        boolQueryBuilder.must(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);

        // 聚合组
        TermsAggregationBuilder countByClassId = AggregationBuilders.terms("countByClass_id");
        countByClassId.field("class_id");
        countByClassId.size(10);
        searchSourceBuilder.aggregation(countByClassId);

        // 分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);

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

        System.out.println("------------------------------------");
        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation countByClassId1 = aggregations.getTermsAggregation("countByClass_id");
        List<TermsAggregation.Entry> buckets = countByClassId1.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println(bucket.getKey() + ":" + bucket.getCount() + " --" + bucket.getName());
        }

        jestClient.shutdownClient();

    }
}
