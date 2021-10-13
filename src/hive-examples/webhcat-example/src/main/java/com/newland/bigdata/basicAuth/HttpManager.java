package com.newland.bigdata.basicAuth;

import com.newland.bigdata.utils.FileUtil;
import com.newland.bigdata.utils.MyHttpDelete;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * HttpManager
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class HttpManager {
    private Logger LOG = LoggerFactory.getLogger(HttpManager.class);

    /**
     * sendHttpGetRequest
     *
     * @param httpClient    HttpClient
     * @param operationUrl  String
     * @param operationName String
     * @return 结果
     */
    public String sendHttpGetRequest(HttpClient httpClient, String operationUrl, String operationName) {
        HttpResponse httpResponse = sendHttpGetRequestGetHttpResponse(httpClient
                , operationUrl, operationName);
        // 处理httpGet响应
        String responseLineContent = handleHttpResponse(httpResponse, operationName);
        LOG.info("SendHttpGetRequest completely.");
        return responseLineContent;
    }

    /**
     * sendHttpGetRequestGetHttpResponse
     *
     * @param httpClient    HttpClient
     * @param operationUrl  String
     * @param operationName String
     * @return HttpResponse
     */
    public HttpResponse sendHttpGetRequestGetHttpResponse(HttpClient httpClient
            , String operationUrl, String operationName) {
        // 校验
        check(operationUrl, operationName);

        try {
            HttpGet httpGet = new HttpGet(operationUrl);
            httpGet.addHeader("Content-Type", "application/json;charset=UTF-8");

            return httpClient.execute(httpGet);
        } catch (HttpResponseException e) {
            LOG.error("HttpResponseException.", e);
        } catch (ClientProtocolException e) {
            LOG.error("ClientProtocolException.", e);
        } catch (IOException e) {
            LOG.error("IOException.", e);
        }

        return null;
    }

    /**
     * sendHttpPostRequestWithString
     *
     * @param httpClient    HttpClient
     * @param operationUrl  String
     * @param jsonString    String
     * @param operationName String
     * @return 结果
     */
    public String sendHttpPostRequestWithString(HttpClient httpClient, String operationUrl
            , String jsonString, String operationName) {
        HttpResponse httpResponse = sendHttpPostRequestWithStringGetHttpResponse(httpClient
                , operationUrl, jsonString, operationName);
        // 处理httpGet响应
        String responseLineContent = handleHttpResponse(httpResponse, operationName);
        LOG.info("SendHttpPostRequest completely.");
        return responseLineContent;
    }

    /**
     * sendHttpPostRequestWithString
     *
     * @param httpClient    HttpClient
     * @param operationUrl  String
     * @param jsonString    String
     * @param operationName String
     * @return HttpResponse
     */
    public HttpResponse sendHttpPostRequestWithStringGetHttpResponse(HttpClient httpClient
            , String operationUrl, String jsonString, String operationName) {
        // 校验
        check(operationUrl, operationName);

        try {
            HttpPost httpPost = new HttpPost(operationUrl);
            httpPost.addHeader("Content-Type", "application/json;charset=UTF-8");
            if (StringUtils.isNotEmpty(jsonString)) {
                httpPost.setEntity(new StringEntity(jsonString, "UTF-8"));
            }

            return httpClient.execute(httpPost);
        } catch (UnsupportedEncodingException e1) {
            LOG.error("UnsupportedEncodingException", e1);
        } catch (ClientProtocolException e1) {
            LOG.error("ClientProtocolException", e1);
        } catch (IOException e) {
            LOG.error("IOException", e);
        }

        return null;
    }

    /**
     * sendHttpPutRequestWithString
     *
     * @param httpclient    HttpClient
     * @param operationUrl  String
     * @param jsonString    String
     * @param operationName String
     * @return String
     */
    public String sendHttpPutRequestWithString(HttpClient httpclient
            , String operationUrl, String jsonString, String operationName) {
        HttpResponse httpResponse = sendHttpPutRequestWithStringGetHttpResponse(httpclient
                , operationUrl, jsonString, operationName);
        // 处理httpGet响应
        String responseLineContent = handleHttpResponse(httpResponse, operationName);
        LOG.info("sendHttpPutRequest completely.");
        return responseLineContent;
    }

    /**
     * sendHttpPutRequestWithStringGetHttpResponse
     *
     * @param httpclient    HttpClient
     * @param operationUrl  String
     * @param jsonString    String
     * @param operationName String
     * @return HttpResponse
     */
    public HttpResponse sendHttpPutRequestWithStringGetHttpResponse(HttpClient httpclient
            , String operationUrl, String jsonString, String operationName) {
        // 校验
        check(operationUrl, operationName);

        try {
            HttpPut httpPut = new HttpPut(operationUrl);
            httpPut.addHeader("Content-Type", "application/json;charset=UTF-8");
            if (StringUtils.isNotEmpty(jsonString)) {
                httpPut.setEntity(new StringEntity(jsonString, "UTF-8"));
            }

            return httpclient.execute(httpPut);
        } catch (UnsupportedEncodingException e1) {
            LOG.error("UnsupportedEncodingException", e1);
        } catch (ClientProtocolException e1) {
            LOG.error("ClientProtocolException", e1);
        } catch (IOException e) {
            LOG.error("IOException", e);
        }

        return null;
    }

    /**
     * sendHttpDeleteRequest
     *
     * @param httpClient    HttpClient
     * @param operationUrl  String
     * @param jsonString    String
     * @param operationName String
     */
    public String sendHttpDeleteRequest(HttpClient httpClient, String operationUrl
            , String jsonString, String operationName) {
        HttpResponse httpResponse = sendHttpDeleteRequestGetHttpResponse(httpClient
                , operationUrl, jsonString, operationName);
        // 处理httpGet响应
        String responseLineContent = handleHttpResponse(httpResponse, operationName);
        LOG.info(String.format("sendHttpDeleteMessage for %s completely.", operationName));
        return responseLineContent;
    }

    /**
     * sendHttpDeleteRequestGetHttpResponse
     *
     * @param httpClient    HttpClient
     * @param operationUrl  String
     * @param jsonString    String
     * @param operationName String
     * @return HttpResponse
     */
    public HttpResponse sendHttpDeleteRequestGetHttpResponse(HttpClient httpClient
            , String operationUrl, String jsonString, String operationName) {
        // 校验
        check(operationUrl, operationName);

        try {
            HttpResponse httpResponse = null;

            if (StringUtils.isEmpty(jsonString)) {
                HttpDelete httpDelete = new HttpDelete(operationUrl);
                httpResponse = httpClient.execute(httpDelete);
            } else {
                MyHttpDelete myHttpDelete = new MyHttpDelete(operationUrl);
                myHttpDelete.addHeader("Content-Type", "application/json;charset=UTF-8");
                myHttpDelete.setEntity(new StringEntity(jsonString, "UTF-8"));
                httpResponse = httpClient.execute(myHttpDelete);
            }
            return httpResponse;
        } catch (ClientProtocolException e1) {
            LOG.error("ClientProtocolException", e1);
        } catch (IOException e) {
            LOG.error("IOException", e);
        } catch (Exception e) {
            LOG.error("Exception", e);
        }

        return null;
    }

    /**
     * 获取返回信息
     *
     * @param httpResponse  http返回
     * @param operationName 操作名称
     * @return 返回信息
     */
    public String handleHttpResponse(HttpResponse httpResponse, String operationName) {
        String lineContent = "";
        if (httpResponse == null) {
            LOG.error("The httpResponse is empty.");
            throw new NullPointerException("The httpResponse is empty.");
        }
        if ((operationName == null) || (operationName.isEmpty())) {
            LOG.error("The operationName is empty.");
            operationName = "UserOperation";
        }
        BufferedReader bufferedReader = null;
        InputStream inputStream = null;
        try {
            LOG.info(String.format("The %s status is %s.", operationName, httpResponse.getStatusLine()));
            // 正常模式
            // application/json;charset=UTF-8
            // 下载模式
            // application/x-download;charset=UTF-8
            // 文件名
            // attachment;filename="yz_newland_1632625100790_keytab.tar"
            String contentType = null;
            String fileName = null;
            for (Header header : httpResponse.getAllHeaders()) {
                if (header.getName().equals("Content-Type")) {
                    contentType = header.getValue().split(";")[0];
                } else if (header.getName().equals("Content-Disposition")) {
                    fileName = header.getValue().split(";")[1].split("=")[1].replace("\"", "");
                }
            }
            HttpEntity entity = httpResponse.getEntity();
            if (entity != null && entity.getContent() != null) {
                // 内容
                inputStream = entity.getContent();
                // 下载
                if (contentType != null && contentType.contains("x-download")) {
                    // get file from input stream
                    String filePath = "d:\\tmp\\chm\\";
                    FileOutputStream fileOutputStream = new FileOutputStream(filePath + fileName);
                    FileUtil.copyBytes(inputStream, fileOutputStream);
                    lineContent = "Download file is " + filePath + fileName;
                    LOG.info(String.format("Download file is %s.", filePath + fileName));
                } else { // 非下载
                    // get content from input stream
                    bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    lineContent = bufferedReader.readLine();
                    LOG.info(String.format("The response lineContent is %s.", lineContent));
                }
            }
        } catch (IOException e) {
            LOG.warn("ReadLine failed.");
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    LOG.info("Close bufferedReader failed.");
                }
            }
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    LOG.info("Close inputStream failed.");
                }
            }
        }
        return lineContent;
    }

    /**
     * 简单解析Http返回
     *
     * @param httpResponse
     * @throws IOException
     */
    public void handleHttpResponse(HttpResponse httpResponse) throws IOException {
        InputStream is = null;
        try {
            is = httpResponse.getEntity().getContent();
            LOG.info(String.format("Status code : %s", httpResponse.getStatusLine().getStatusCode()));
            LOG.info(String.format("message is : %s", Arrays.deepToString(httpResponse.getAllHeaders())));
            LOG.info(String.format("结果 : %s", new String(IOUtils.toByteArray(is), StandardCharsets.UTF_8)));
        } finally {
            if (is != null) is.close();
        }
    }

    /**
     * 校验
     *
     * @param operationUrl  地址
     * @param operationName 操作名称
     */
    private void check(String operationUrl, String operationName) {
        LOG.info(String.format("Enter userOperation %s.", operationName));
        if ((operationUrl == null) || (operationUrl.isEmpty())) {
            LOG.error("The operationUrl is empty.");
            throw new NullPointerException("The operationUrl is empty.");
        }
        LOG.info(String.format("The operationUrl is:%s", operationUrl));
    }
}
