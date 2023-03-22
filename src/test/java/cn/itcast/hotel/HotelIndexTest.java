package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

/**
 * @author MXK
 * @version 1.0
 * @description 酒店索引测试
 * @date 2023/3/22 16:41
 */

public class HotelIndexTest {
    private RestHighLevelClient client;

    /**
     * @return void
     * @description 初始化JavaRestClient
     * @author MXK
     * @date 2023/3/22 16:46
     */
    @Test
    void testInit() {
        System.out.println(client);
    }

    @Test
    void createHotelIndex() throws IOException {
        // 1. 创建Request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        // 2. 准备请求的参数 DSL语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        // 3. 发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteHotelIndex() throws IOException {
        // 1. 创建request对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        // 2. 发起请求
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testExistsHotelIndex() throws IOException {
        // 1. 创建request对象
        GetIndexRequest request = new GetIndexRequest("hotel");
        // 2. 判断索引是否存在
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://8.130.102.97:9200")
        ));
    }

    @AfterEach
    void testDown() throws IOException {
        this.client.close();
    }
}

    