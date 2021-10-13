package com.newland.bigdata.utils;

import org.apache.http.HttpResponse;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class Krb5AuthActionTest {
    private String principal = "admin@HADOOP.COM";
    private String keytab = null;// 不需要keytab
    private String krb5Location = "d:\\tmp\\etc\\keytab\\krb5.conf";
    private String baseUrl = "https://10.1.12.79:21055";
    private Krb5AuthAction krb5AuthAction;

    @Before
    public void setUp() throws Exception {
        krb5AuthAction = new Krb5AuthAction(principal, keytab, krb5Location, false);
        krb5AuthAction.kinit("123!!Qwe");
    }

    @Test
    public void sendHttpGetRequest() throws IOException {
        // 获取hive服务器状态
        String url = "/templeton/v1/status";
        // 获取所有数据库
        url = "/templeton/v1/ddl/database";
        HttpResponse response = krb5AuthAction.sendHttpGetRequest(baseUrl + url, null);
        krb5AuthAction.handleHttpResponse(response);
    }

    @Test
    public void sendHttpPutRequestWithString() throws IOException {
        // 创建hive数据库
        String url = "/templeton/v1/ddl/database/test2";
        String jsonString = "{\"location\": \"/user/bdoc/nl_lv1/nl_lv1_p/hive/test2\", \"comment\": \"test2\"}";
        HttpResponse response = krb5AuthAction.sendHttpPutRequestWithString(baseUrl + url
                , jsonString, null);
        krb5AuthAction.handleHttpResponse(response);
    }
}