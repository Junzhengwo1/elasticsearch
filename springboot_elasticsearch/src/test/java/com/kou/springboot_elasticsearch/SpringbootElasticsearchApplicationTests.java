package com.kou.springboot_elasticsearch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kou.pojo.User;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class SpringbootElasticsearchApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    @Test
    public void find(){
        System.out.println(restHighLevelClient);
    }

    @Test
    void contextLoads() throws IOException {
        //创建索引
        CreateIndexRequest request = new CreateIndexRequest("kou2");
        //客户端执行请求 获得响应
        CreateIndexResponse createIndexResponse =
                restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);

    }

    /**
     * 判断索引是否存在
     * @throws IOException
     */
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request=new GetIndexRequest("kou");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 删除索引
     * @throws IOException
     */
    @Test
    void deleteTest() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("kou2");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    /**
     * 测试添加文档 即是说 把一个User对象加入到索引中
     */
    @Test
    void testAddDocument() throws IOException {
        //测试添加文档
        User user = new User();
        user.setName("kou");
        user.setAge(18);
        //创建请求
        IndexRequest request = new IndexRequest("king");
        // 规则 put/wang/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        //将我们的数据放入请求中 json数据
        //首先需要将我们的user对象转换成json数据后放入请求中。

        /*
        //jackson将对象转换程json
        ObjectMapper mapper=new ObjectMapper();
        String s = mapper.writeValueAsString(user);
        System.out.println(s);
        */
        String jsonUser = JSON.toJSONString(user);
        System.out.println(jsonUser);
        User user1 = JSON.parseObject(jsonUser, User.class);
        System.out.println(user1);
        request.source(JSON.toJSONString(user), XContentType.JSON);
        //客户端发送请求,获取响应的结果
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        System.out.println(index.toString());
        System.out.println(index.status());

    }

    /**
     * 获取文档，判断是否存在 get/index/doc/1
     * @throws IOException
     */
    @Test
    void testGetDocumentIsExist() throws IOException {
        GetRequest getRequest=new GetRequest("king","1");

        //不获取_source 的上下文了。
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 获取文档的内容
     * @throws IOException
     */
    @Test
    void testGetDocumentInformation() throws IOException {
        GetRequest getRequest=new GetRequest("king","1");
        GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());
        System.out.println(response);

    }

    /**
     * 更新文档的记录
     */
    @Test
    void testUpdateDocumentInformation() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("wang", "1");
        updateRequest.timeout("1s");

        User user = new User();
        user.setName("王后");
        user.setAge(25);
        updateRequest.doc(JSON.toJSONString(user),XContentType.JSON);
        UpdateResponse response = restHighLevelClient.update(updateRequest,RequestOptions.DEFAULT);
        System.out.println(response.status());

    }

    /**
     * 删除文档的记录
     */
    @Test
    void testDeleteDocumentInformation() throws IOException {
        DeleteRequest request = new DeleteRequest("wang", "1");
        request.timeout("1s");

        DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    /**
     * 大批量插入数据
     */
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        ArrayList<User> userArrayList=new ArrayList<>();
        User user1 = new User();
        User user2 = new User();
        User user3 = new User();
        User user4 = new User();
        user1.setName("kou1");
        user1.setAge(21);
        user2.setName("kou2");
        user2.setAge(22);
        user3.setName("kou3");
        user3.setAge(23);
        user4.setName("kou4");
        user4.setAge(24);
        userArrayList.add(user1);
        userArrayList.add(user2);
        userArrayList.add(user3);
        userArrayList.add(user4);
        //批处理
        for (int i = 0; i <userArrayList.size() ; i++) {
            bulkRequest.add(new IndexRequest("wang")
                    .id(""+(i+1))
                    .source(JSON.toJSONString(userArrayList.get(i)),XContentType.JSON));
        }
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.hasFailures());//是否失败 返回false表示成功。
    }

    /**
     * 查询
     */
    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest=new SearchRequest("wang");
        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //查询条件，我们可以使用QueryBuilders工具类来匹配查询条件。
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "kou2");
        sourceBuilder.query(termQueryBuilder);
        //分页操作
        sourceBuilder.from();
        sourceBuilder.size();

        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("============");
        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }

    }

}
