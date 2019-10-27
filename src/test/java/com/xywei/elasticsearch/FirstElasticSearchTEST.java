package com.xywei.elasticsearch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;

/**
 * @Description 第一个ES程序
 * @Author future
 * @DateTime 2019/10/26 17:20
 */
public class FirstElasticSearchTEST {

    private static final String CLUSTER_NATE = "my-es-application";
    private static final String HOST_IP = "127.0.0.1";
    private static TransportClient client = null;

    @Before
    public void initClient() throws Exception {

        InetSocketTransportAddress address1 = new InetSocketTransportAddress(InetAddress.getByName(HOST_IP), 9301);
        InetSocketTransportAddress address2 = new InetSocketTransportAddress(InetAddress.getByName(HOST_IP), 9302);
        InetSocketTransportAddress address3 = new InetSocketTransportAddress(InetAddress.getByName(HOST_IP), 9303);
        Settings settings = Settings.builder().put("cluster.name", CLUSTER_NATE)
                .build();
        client = new PreBuiltTransportClient(settings);
//        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST_IP), 9301));
        client.addTransportAddresses(address1, address2, address3);

        if (null == client) {
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>client为空>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            System.exit(1);
        }

    }

    @After
    public void closeClient() {
        if (null != client) {
            client.close();
            System.out.println(">>>>>>>>>>>>>>>>>>>>>>client closed>>>>>>>>>>>>>>>>>>>>");
        }
    }

    @Test
    public void testClient() {
        System.out.println("client:>>>>>>>>>>>>>>>" + client);
    }

    /**
     * @Description 测试添加document
     * @Author future
     * @DateTime 2019/10/27 0:24
     **/
    @Test
    public void testInsert() throws Exception {

        //准备数据
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "D")
                .field("age", "40")
                .field("dept", "Java")
                .startObject("address")
                .field("country", "US")
                .field("province", "NY")
                .field("city", "HY")
                .endObject()
                .endObject();

        IndexResponse response = client.prepareIndex("company", "employee", "004")
                .setSource(xContentBuilder).get();

        System.out.println("insert>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + response.getVersion());
    }

    /**
     * @Description 批量插入document 方法1，TODO 是否还有其他方法？
     * @Author future
     * @DateTime 2019/10/27 0:59
     **/
    @Test
    public void testInsertBatch() throws Exception {

        client.prepareIndex("school", "Python", "001")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject().field("name", "A").field("age", "18").field("gender", "W").endObject()
                ).get();

        client.prepareIndex("school", "Python", "002")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject().field("name", "B").field("age", "25").field("gender", "M").endObject()
                ).get();

        client.prepareIndex("school", "Python", "003")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject().field("name", "C").field("age", "30").field("gender", "M").endObject()
                ).get();

        client.prepareIndex("school", "Python", "004")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject().field("name", "D").field("age", "40").field("gender", "W").endObject()
                ).get();

        client.prepareIndex("school", "Python", "005")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject().field("name", "E").field("age", "40").field("gender", "W").endObject()
                ).get();

        client.prepareIndex("school", "Python", "006")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject().field("name", "F").field("age", "30").field("gender", "W").endObject()
                ).get();

        client.prepareIndex("school", "Python", "007")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject().field("name", "G").field("age", "30").field("gender", "M").endObject()
                ).get();


    }

    /**
     * @Description 查询数据
     * @Author future
     * @DateTime 2019/10/27 1:09
     **/
    @Test
    public void testGet() {

        //查询单条
        GetResponse response = client.prepareGet("school", "Java", "003").get();
        System.out.println(">>>>>>>>>>>>>>>" + response);

        //多条
        SearchResponse school = client.prepareSearch("school")
                .setTypes("Python")
                .get();
        System.out.println(">>>>>>>>>>>>>>>" + school);
    }

    /**
     * @Description 测试通过ID查询文档，相当于SQL的in
     * @Author future
     * @DateTime 2019/10/27 17:23
     **/
    @Test
    public void testFindByIds() {

//        构建查询
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("001", "002");
//        执行查询
        SearchResponse searchResponse = client.prepareSearch("school")
//                .setTypes("Python")
                .setQuery(queryBuilder)
                .get();
        //获取结果数据
        SearchHits hits = searchResponse.getHits();
        System.out.println("记录数" + hits.getTotalHits());

        //遍历所有的对象
        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            SearchHit next = iterator.next();
            System.out.println(">>>>>>>>>>>数据有>>>>>>>>>>>>" + next.getType() + next.getId() + ":" + next.getSourceAsString());
        }


    }

    /**
     * @Description 相当于SQL的条件查询where username="xx"，单个条件
     * @Author future
     * @DateTime 2019/10/27 17:58
     **/
    @Test
    public void queryByTerm() {
        QueryBuilder queryBuilder = QueryBuilders.termQuery("age", "30");
        SearchResponse searchResponse = client.prepareSearch("school")
                .setTypes("Java")
                .setQuery(queryBuilder)
                .get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("结果有" + hits.getTotalHits());
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit next = iterator.next();
            System.out.println(next.getType() + next.getId() + ":" + next.getSourceAsString());
        }
    }

    /**
     * @Description 类似SQL Like
     * @Author future
     * @DateTime 2019/10/27 19:14
     **/
    @Test
    public void testQueryString() {

        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("40").defaultField("age");
//        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("W").defaultField("gender");

        SearchResponse response = client.prepareSearch("school").setTypes("Java").setQuery(queryBuilder).get();

        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());

        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()) {
            SearchHit next = iterator.next();
            System.out.println(next.getType() + next.getId() + ":" + next.getSourceAsString());
        }

    }

    /**
     * @Description 测试分页
     * @Author future
     * @DateTime 2019/10/27 23:00
     **/
    @Test
    public void testPage() {

        SearchResponse searchResponse = client.prepareSearch("school")
                .setTypes("Java")
                //设置分页
                .setFrom(0)
                .setSize(3)
                .get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("所有记录数" + hits.getTotalHits());
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit next = iterator.next();
            System.out.println("数据都有：" + next.getType() + "**" + next.getSourceAsString());
        }


    }


    /**
     * @Description 测试高亮显示
     * @Author future
     * @DateTime 2019/10/27 23:44
     **/
    @Test
    public void testHighlight() {

    }

}
