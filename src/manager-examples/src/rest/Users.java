package rest;

import basicAuth.BasicAuthAccess;
import basicAuth.HttpManager;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

/**
 * 获取用户列表
 *
 * @author chenqixu
 */
public class Users {
    private static final Logger LOG = LoggerFactory.getLogger(Users.class);

    private static final String URL = "api/v2/permission/users";

    /**
     * 程序运行入口
     *
     * @param args 参数
     */
    public static void main(String[] args) {
        LOG.info("Enter main.");
        // 文件UserInfo.properties的路径
        String userFilePath = System.getProperty("user.dir") + "/src/manager-examples/conf/UserInfo.properties";

        InputStream userInfo = null;
        ResourceBundle resourceBundle = null;
        try {
            File file = new File(userFilePath);
            if (!file.exists()) {
                LOG.error("The user info file doesn't exist.");
                return;
            }

            LOG.info("Get the web info and user info from file {} ", file);

            userInfo = new BufferedInputStream(new FileInputStream(file));
            resourceBundle = new PropertyResourceBundle(userInfo);

            // 获取用户名
            String userName = resourceBundle.getString("userName");
            LOG.info("The user name is : {}.", userName);
            if (userName == null || userName.isEmpty()) {
                LOG.error("The userName is empty.");
            }

            // 获取用户密码
            String password = resourceBundle.getString("password");
            if (password == null || password.isEmpty()) {
                LOG.error("The password is empty.");
            }

            String webUrl = resourceBundle.getString("webUrl");
            LOG.info("The webUrl is : {}.", webUrl);
            if (password == null || password.isEmpty()) {
                LOG.error("The password is empty.");
            }

            // userTLSVersion是必备的参数，是处理jdk1.6服务端连接jdk1.8服务端时的重要参数，如果用户使用的是jdk1.8该参数赋值为空字符串即可
            String userTLSVersion = "TLSv1.2";// TLSv1.1

            // 调用firstAccess接口完成登录认证
            LOG.info("Begin to get httpclient and first access.");
            BasicAuthAccess authAccess = new BasicAuthAccess();
            HttpClient httpClient = authAccess.loginAndAccess(webUrl, userName, password, userTLSVersion);

            LOG.info("Start to access REST API.");

            // 查询用户列表
            String operationName = "";
            String exportOperationUrl = webUrl + URL;
            HttpManager httpManager = new HttpManager();
            // 调用接口
            String responseLineContent = httpManager.sendHttpGetRequest(httpClient, exportOperationUrl, operationName);
            LOG.info("responseLineContent：{}", responseLineContent);
            LOG.info("Exit main.");

        } catch (FileNotFoundException e) {
            LOG.error("File not found exception.");
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        } catch (Throwable e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        } finally {
            if (userInfo != null) {
                try {
                    userInfo.close();
                } catch (IOException e) {
                    LOG.error("IOException.");
                }
            }
        }
    }
}
