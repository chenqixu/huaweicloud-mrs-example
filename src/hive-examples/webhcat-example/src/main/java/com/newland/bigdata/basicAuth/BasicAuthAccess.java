package com.newland.bigdata.basicAuth;

import com.newland.bigdata.basicAuth.exception.*;
import com.newland.bigdata.utils.WebClientDevWrapper;
import com.newland.bigdata.validutil.ParamsValidUtil;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.codec.Base64;
import sun.misc.BASE64Encoder;

import java.io.*;

/**
 * BasicAuthAccess
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class BasicAuthAccess {
    private static final Logger LOG = LoggerFactory.getLogger(BasicAuthAccess.class);

    /**
     * loginAndAccess，华为jdk默认1.8，所以这里强制TLSv1.2
     *
     * @param webUrl   url
     * @param userName 用户名
     * @param password 密码
     * @return 响应结果
     * @throws InvalidInputParamException       异常
     * @throws GetClientException               异常
     * @throws WrongUsernameOrPasswordException 异常
     * @throws FirstTimeAccessException         异常
     * @throws AuthenticationException          异常
     */
    public HttpClient loginAndAccess(String webUrl, String userName, String password)
            throws InvalidInputParamException, GetClientException, WrongUsernameOrPasswordException,
            FirstTimeAccessException, AuthenticationException {
        return loginAndAccess(webUrl, userName, password, "TLSv1.2");
    }

    /**
     * loginAndAccess
     *
     * @param webUrl         url
     * @param userName       用户名
     * @param password       密码
     * @param userTLSVersion String
     * @return 响应结果
     * @throws InvalidInputParamException       异常
     * @throws GetClientException               异常
     * @throws WrongUsernameOrPasswordException 异常
     * @throws FirstTimeAccessException         异常
     * @throws AuthenticationException          异常
     */
    private HttpClient loginAndAccess(String webUrl, String userName, String password, String userTLSVersion)
            throws InvalidInputParamException, GetClientException, WrongUsernameOrPasswordException,
            FirstTimeAccessException, AuthenticationException {
        LOG.info("Enter loginAndAccess.");
        if (ParamsValidUtil.isEmpty(new String[]{webUrl, userName, password})) {
            LOG.error("Invalid input param.");
            throw new InvalidInputParamException("Invalid input param.");
        }

        if ((userTLSVersion == null) || (userTLSVersion.isEmpty())) {
            userTLSVersion = "TLS";
        }

        LOG.info(String.format("1.Get http client for sending https request, username is %s, webUrl is %s.", userName, webUrl));
        HttpClient httpClient = getHttpClient(userTLSVersion);
        LOG.info(String.format("The new http client is: %s.", httpClient));
        if (ParamsValidUtil.isNull(new Object[]{httpClient})) {
            LOG.error("Get http client error.");
            throw new GetClientException("Get http client error.");
        }

        LOG.info(String.format("2.Construct basic authentication,username is %s.", userName));
        // 使用password认证
        String credentials = password;
        // 使用keytab认证
        // String credentials = getKeytabContent("E:\\user.keytab");
        String authentication = constructAuthentication(userName, credentials);
        if (ParamsValidUtil.isNull(new Object[]{authentication})) {
            LOG.error("Authroize failed.");
        }

        LOG.info(String.format("3. Send first access request, usename is %s.", userName));
        HttpResponse firstAccessResp = firstAccessResp(webUrl, userName, authentication, httpClient);

        if (ParamsValidUtil.isNull(new Object[]{firstAccessResp})) {
            LOG.error("First access response error.");
            throw new FirstTimeAccessException("First access response error.");
        }

        return httpClient;
    }

    private HttpClient getHttpClient(String userTLSVersion) {
        LOG.info("Enter getHttpClient.");

        ThreadSafeClientConnManager ccm = new ThreadSafeClientConnManager();
        ccm.setMaxTotal(100);

        HttpClient httpclient = WebClientDevWrapper.wrapClient(new DefaultHttpClient(ccm), userTLSVersion);
        LOG.info("Exit getHttpClient.");
        return httpclient;
    }

    private String constructAuthentication(String userName, String credentials) throws FirstTimeAccessException {
        StringBuffer sb = new StringBuffer();
        sb.append("Basic");
        sb.append(" ");

        String userNamePasswordToken = userName + ":" + credentials;
        try {
            byte[] token64 = Base64.encode(userNamePasswordToken.getBytes("UTF-8"));
            String token = new String(token64);
            sb.append(token);
        } catch (UnsupportedEncodingException e) {
            LOG.error("First access failed because of UnsupportedEncodingException.");
            throw new FirstTimeAccessException("UnsupportedEncodingException");
        }
        LOG.info(String.format("the authentication is %s .", sb.toString()));
        return sb.toString();
    }

    public static String getKeytabContent(String keytabPath) {
        FileInputStream inputStream = null;
        StringBuilder sb = new StringBuilder();
        try {
            inputStream = new FileInputStream(keytabPath);

            // Array size is 1 to prevent write overflow
            byte[] tmp = new byte[1];
            while ((inputStream.read(tmp)) != -1) {
                BASE64Encoder encoder = new BASE64Encoder();
                sb.append(new String(Base64.encode(tmp)));
            }
        } catch (IOException e) {
            return null;
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                }
            }
        }
        System.out.println(sb.toString());
        return sb.toString();
    }

    private HttpResponse firstAccessResp(String webUrl, String userName, String authentication, HttpClient httpClient)
            throws WrongUsernameOrPasswordException, FirstTimeAccessException, AuthenticationException {
        HttpGet httpGet = new HttpGet(webUrl + "api/v2/session/status");
        httpGet.addHeader("Authorization", authentication);
        BufferedReader bufferedReader = null;

        HttpResponse response = null;
        try {
            response = httpClient.execute(httpGet);
            String stateLine = response.getStatusLine().toString();
            LOG.info(String.format("First access status is %s", stateLine));

            InputStream inputStream = response.getEntity().getContent();
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String lineContent = "";
            lineContent = bufferedReader.readLine();
            LOG.info(String.format("Response content is %s ", lineContent));

            if (!(stateLine.equals("HTTP/1.1 200 "))) {
                throw new AuthenticationException("Authorize failed!");
            }

            LOG.info(String.format("User %s first access success", userName));

            while (lineContent != null) {
                LOG.debug(String.format("lineContent=%s", lineContent));

                if (lineContent.contains("The credentials you provided cannot be determined to be authentic")) {
                    LOG.error("The username or password is wrong");
                    throw new WrongUsernameOrPasswordException("The username or password is wrong");
                }

                if (lineContent.contains("modify_password.html")) {
                    LOG.warn("First access, please reset password");
                    throw new FirstTimeAccessException("First access, please reset password");
                }
                lineContent = bufferedReader.readLine();
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error("First access server failed because of UnsupportedEncodingException.");
            throw new FirstTimeAccessException("UnsupportedEncodingException");
        } catch (ClientProtocolException e) {
            LOG.error("First access server failed because of FirstTimeAccessException.");
            throw new FirstTimeAccessException("ClientProtocolException");
        } catch (IOException e) {
            LOG.error("First access server failed because of ClientProtocolException.");
            throw new FirstTimeAccessException("IOException");
        } catch (IllegalStateException e) {
            LOG.error("First access server failed because of IllegalStateException.");
            throw new FirstTimeAccessException("IllegalStateException");
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    LOG.warn("Close buffer reader failed.");
                }
            }
        }
        return response;
    }
}
