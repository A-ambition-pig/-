package com.nanfeng.gmall.publisher.service.impl;

import com.nanfeng.gmall.publisher.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ESServiceImpl implements ESService {

    //将ES客户端操作对象注入service
    @Autowired
    JestClient jestClient;

    /**
     *
     * @param date
     * @return
     */
    @Override
    public Long getDauTotal(String date) {
        //返回值
        Long total = 0L;

        String indexName = "gmall0523_dau_info_" + date + "-query";

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchAllQueryBuilder());
        String query = sourceBuilder.toString();
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();

        try {
            SearchResult result = jestClient.execute(search);
            total = result.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败");
        }

        return total;
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        Map<String, Long> hourMap = new HashMap<>();

        String indexName = "gmall0523_dau_info_" + date + "-query";

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("groupBy_hr", ValueType.LONG)
                .field("hr")
                .size(24);
        //String aggregationBuilders = AggregationBuilders.terms("groupBy_hr").size(24).field();
        sourceBuilder.aggregation(termsAggregationBuilder);

        String query = sourceBuilder.toString();
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();

        try {
            SearchResult res = jestClient.execute(search);
            TermsAggregation termsAgg = res.getAggregations().getTermsAggregation("groupBy_hr");
            if (null != termsAgg) {
                List<TermsAggregation.Entry> buckets = termsAgg.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    hourMap.put(bucket.getKey(), bucket.getCount());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询异常");
        }

        return hourMap;
    }
}
