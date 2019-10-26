package com.xywei.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

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
        }
    }

    @Test
    public void testClient() {
        System.out.println("client:>>>>>>>>>>>>>>>" + client);
    }
}
