package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

/**
 * @author MXK
 * @version 1.0
 * @description 酒店索引测试
 * @date 2023/3/22 16:41
 */
@SpringBootTest
public class HotelDocumentTest {
    private RestHighLevelClient client;

    @Resource
    private IHotelService hotelService;

    @Test
    void testAddDocument() throws IOException {
        // 根据id查询酒店数据
        Hotel hotel = hotelService.getById(61083L);
        // 转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);

        // 1. 准备Request对象
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        // 2. 准备json文档
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        // 3. 发送请求
        client.index(request, RequestOptions.DEFAULT);
    }

    @Test
    void testGetDocumentById() throws IOException {
        // 1. 准备request
        GetRequest request = new GetRequest("hotel", "61083");
        // 2. 发送请求，得到响应
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 3. 解析响应结果
        String json = response.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    @Test
    void testUpdateDocument() throws IOException {
        // 1. request
        UpdateRequest request = new UpdateRequest("hotel", "61083");

        // 2. 请求参数
        request.doc(
                "price", "999",
                "starName", "四钻"
        );
        // 3. 发送请求
        client.update(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteDocument() throws IOException {
        // 1. request
        DeleteRequest request = new DeleteRequest("hotel", "61083");

        // 2. 发送请求
        client.delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void Bulk() throws IOException {

        // 1. 创建Bulk请求
        BulkRequest request = new BulkRequest();

        // 批量查询酒店数据
        List<Hotel> hotels = hotelService.list();
        // 转换为文档类型
        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            // 2. 准备参数
            request.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc), XContentType.JSON));
        }

        // 3. 发送请求
        client.bulk(request, RequestOptions.DEFAULT);
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

    