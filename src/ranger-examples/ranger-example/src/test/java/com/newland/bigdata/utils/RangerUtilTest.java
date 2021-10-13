package com.newland.bigdata.utils;

import com.alibaba.fastjson.JSON;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.view.RangerPolicyList;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

public class RangerUtilTest {
    private Logger LOG = LoggerFactory.getLogger(RangerUtilTest.class);
    private RangerUtil rangerUtil;

    @Before
    public void setUp() throws Exception {
        rangerUtil = RangerUtil.getInstance();
    }

    /**
     * 查询ranger所有服务
     */
    @Test
    public void queryRangerAllServices() {
        String url = "https://10.1.12.80:21405/service/plugins/services";
        ClientResponse response = rangerUtil.accept(url);
        rangerUtil.handleClientResponse(response);
    }

    /**
     * 查询指定id的ranger服务信息
     *
     * @param id 服务id
     */
    @Test
    public void queryRangerServices(String id) {
        String url = "https://10.1.12.80:21405/service/plugins/services/" + id;
        ClientResponse response = rangerUtil.accept(url);
        String jsonStr = rangerUtil.handleClientResponse(response, String.class);
        RangerService rangerService = JSON.parseObject(jsonStr, RangerService.class);
        LOG.info(String.format("name：%s，configs：%s", rangerService.getName(), rangerService.getConfigs()));
    }

    /**
     * 根据服务id和数据库名称查询所有符合的策略
     *
     * @param hiveId 服务id
     * @param dbname 数据库名称
     */
    @Test
    public void queryUserByHiveDB(String hiveId, String dbname) throws InvocationTargetException, IllegalAccessException {
        // https://10.1.12.80:21405/service/plugins/policies/service/7?page=0&pageSize=25&total_pages=1&totalCount=9&startIndex=0&policyType=0&resource:database=test&_=1633747798164
        String url = "https://10.1.12.80:21405/service/plugins/policies/service/"
                + hiveId + "?resource:database=" + dbname + "&isDefaultPolicy=false";
        ClientResponse response = rangerUtil.accept(url);
        String jsonStr = rangerUtil.handleClientResponse(response, String.class);
        jsonStr = jsonStr.replace("policies", "list");
        RangerPolicyList rangerPolicyPList = JSON.parseObject(jsonStr, RangerPolicyList.class);
        LOG.info(String.format("arg：%s，policies：%s", rangerPolicyPList.getTotalCount(), rangerPolicyPList.getPolicies()));
    }
}