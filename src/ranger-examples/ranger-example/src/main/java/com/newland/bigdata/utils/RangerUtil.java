package com.newland.bigdata.utils;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

/**
 * ranger工具
 *
 * @author chenqixu
 */
public class RangerUtil {
    private Logger LOG = LoggerFactory.getLogger(RangerUtil.class);
    private String username = "rangeradmin";
    private String password = "Huawei@123";

    public RangerUtil() {
    }

    public RangerUtil(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public static RangerUtil getInstance() {
        return new RangerUtil();
    }

    public static RangerUtil getInstance(String username, String password) {
        return new RangerUtil(username, password);
    }

    /**
     * 请求
     *
     * @param reqUrl
     * @return
     */
    public ClientResponse accept(String reqUrl) {
        LOG.info("accept, reqUrl=" + reqUrl);
        ClientResponse response = null;
        Client client = null;
        try {
            SSLContext sslContext = WebClientDevWrapper.getSSLContext("TLSv1.2");
            assert sslContext != null;
            ClientConfig clientConfig = new DefaultClientConfig();
            clientConfig.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES
                    , new HTTPSProperties(new MyHostnameVerifier(), sslContext));
            client = Client.create(clientConfig);
            client.addFilter(new HTTPBasicAuthFilter(username, password));
            WebResource webResource = client.resource(reqUrl);
            response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON)
                    .get(ClientResponse.class);
        } catch (Exception e) {
            LOG.error("createWebResource is fail, errMessage=" + e.getMessage());
        } finally {
            if (client != null) {
                client.destroy();
            }
        }
        return response;
    }

    /**
     * 解析返回值
     *
     * @param response
     */
    public void handleClientResponse(ClientResponse response) {
        try {
            if (response != null && response.getStatus() == 200) {
                String jsonString = response.getEntity(String.class);
                LOG.info("request is success , the resp=" + jsonString);
            } else {
                RESTResponse resp = RESTResponse.fromClientResponse(response);
                LOG.warn("request is fail," + resp.toString());
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * 解析返回值
     *
     * @param response
     * @param tClass
     * @param <T>
     * @return
     */
    public <T> T handleClientResponse(ClientResponse response, Class<T> tClass) {
        T t = null;
        try {
            if (response != null && response.getStatus() == 200) {
                t = response.getEntity(tClass);
                LOG.info("request is success , the resp=" + t.toString());
            } else {
                RESTResponse resp = RESTResponse.fromClientResponse(response);
                LOG.warn("request is fail," + resp.toString());
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
        return t;
    }
}
