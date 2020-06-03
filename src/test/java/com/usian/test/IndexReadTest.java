package com.usian.test;

import com.usian.ElasticSearchApp;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {ElasticSearchApp.class})
public class IndexReadTest {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private SearchRequest searchRequest;

    private SearchResponse response;

    //根据id查询document文档
    @Test
    public void getDoc() throws IOException {
        GetRequest getRequest = new GetRequest("java1906b","course","1");
        GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        boolean exists = response.isExists();
        System.out.println(exists);
        String source = response.getSourceAsString();
        System.out.println(source);
    }


    @Before
    public void getSearchRequest(){
        //搜索请求对象，给他索引类型
        searchRequest = new SearchRequest("java1906b");
        searchRequest.types("course");
    }

    //DSL match_all查询
    @Test
    public void getMatch_allDocument() throws IOException {

        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }


    //DSL match_all查询分页查询
    @Test
    public void testSearchPage() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);
        searchSourceBuilder.sort("price", SortOrder.ASC);
        searchRequest.source(searchSourceBuilder);
        response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }


    //match查询指定条件查询
    @Test
    public void testSearchByMatch() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name","spring开发").operator(Operator.AND));
        searchRequest.source(searchSourceBuilder);
        response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //multi_match查询指定条件查询
    @Test
    public void testSearchByMultiMatch() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("spring开发",new String[]{"name","description"}));
        searchRequest.source(searchSourceBuilder);
        response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }
    //multi_match查询指定条件查询
    @Test
    public void testSearchByMultiMatch1() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("开发",new String[]{"name","description"}).operator(Operator.AND));
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);
        searchSourceBuilder.sort("price",SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }


    //bool查询
    @Test
    public void testBoolSearch() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name","开发"));
        boolQueryBuilder.must(QueryBuilders.rangeQuery("price").gte(50).lte(100));
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //filter查询
    @Test
    public void testFilterSearch() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name","开发"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(0).lte(50));
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }


    //highlight高亮显示
    @Test
    public void testHighLightSearch() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("开发","name"));
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<red>");
        highlightBuilder.postTags("</red");
        highlightBuilder.fields().add(new HighlightBuilder.Field("name"));
        searchSourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(searchSourceBuilder);
        response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("共搜索到" + totalHits + "条文档");
        SearchHit[] searchHit = hits.getHits();
        for (SearchHit documentFields : searchHit) {
            String id = documentFields.getId();
            System.out.println(id);
            String sourceAsString = documentFields.getSourceAsString();
            System.out.println(sourceAsString);
            Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();
            if(highlightFields!=null){
                HighlightField name = highlightFields.get("name");
                Text[] fragments = name.getFragments();
                System.out.println("高亮字段：" + fragments[0].toString());
            }
        }
    }

    @After
    public void ParseException(){
        //搜索匹配结果
        SearchHits hits = response.getHits();
        //搜索总记录数
        long totalHits = hits.totalHits;
        System.out.println("共搜索到" + totalHits + "条文档");
        //搜索到的档
        SearchHit[] searchHits = hits.getHits();
        //遍历
        for (SearchHit hit : searchHits) {
            // 文档id
            String id = hit.getId();
            System.out.println("id：" + id);
            // 文档内容
            String source = hit.getSourceAsString();
            System.out.println(source);
        }
    }
}
