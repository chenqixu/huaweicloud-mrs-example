package com.huawei.presto;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Hive帮助类
 *
 * @author chenqixu
 */
public class HiveHelper extends JDBCHelper {
    private static final Logger logger = LoggerFactory.getLogger(HiveHelper.class);
    private final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    private final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = null;

    private Configuration CONF = null;
    private String KRB5_FILE = null;
    private String USER_NAME = null;
    private String USER_KEYTAB_FILE = null;

    /* zookeeper节点ip和端口列表 */
    private String zkQuorum = null;
    private String auth = null;
    private String sasl_qop = null;
    private String zooKeeperNamespace = null;
    private String serviceDiscoveryMode = null;
    private String principal = null;

    /* hive JDBC连接串 */
    private String hiveURL;

    /**
     * hive初始化
     *
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void init() throws IOException, ClassNotFoundException {
        CONF = new Configuration();

        Properties clientInfo = null;
        String userdir =
                System.getProperty("user.dir")
                        + File.separator
                        + "conf"
                        + File.separator;
        InputStream fileInputStream = null;
        try {
            clientInfo = new Properties();
            /**
             * "hiveclient.properties"为客户端配置文件，如果使用多实例特性，需要把该文件换成对应实例客户端下的"hiveclient.properties"
             * "hiveclient.properties"文件位置在对应实例客户端安裝包解压目录下的config目录下
             */
            String hiveclientProp = userdir + "hiveclient.properties";
            File propertiesFile = new File(hiveclientProp);
            fileInputStream = new FileInputStream(propertiesFile);
            clientInfo.load(fileInputStream);
        } catch (IOException e) {
            throw new IOException(e);
        } finally {
            if (fileInputStream != null) {
                fileInputStream.close();
                fileInputStream = null;
            }
        }
        /**
         * zkQuorum获取后的格式为"xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002";
         * "xxx.xxx.xxx.xxx"为集群中ZooKeeper所在节点的业务IP，端口默认是24002
         */
        zkQuorum = clientInfo.getProperty("zk.quorum");
        auth = clientInfo.getProperty("auth");
        sasl_qop = clientInfo.getProperty("sasl.qop");
        zooKeeperNamespace = clientInfo.getProperty("zooKeeperNamespace");
        serviceDiscoveryMode = clientInfo.getProperty("serviceDiscoveryMode");
        principal = clientInfo.getProperty("principal");
        // 设置新建用户的USER_NAME，其中"xxx"指代之前创建的用户名，例如创建的用户为user，则USER_NAME为user
        USER_NAME = "yz_newland"; // 注意需要换成自身项目使用的用户

        if ("KERBEROS".equalsIgnoreCase(auth)) {
            // 设置客户端的keytab和krb5文件路径
            USER_KEYTAB_FILE = "conf/user.keytab";
            KRB5_FILE = userdir + "krb5.conf";
            System.setProperty("java.security.krb5.conf", KRB5_FILE);
            ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/" + KerberosHelper.getUserRealm();
            System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        }

        // 拼接JDBC URL
        StringBuilder strBuilder = new StringBuilder("jdbc:hive2://").append(zkQuorum).append("/default");

        if ("KERBEROS".equalsIgnoreCase(auth)) {
            strBuilder
                    .append(";serviceDiscoveryMode=")
                    .append(serviceDiscoveryMode)
                    .append(";zooKeeperNamespace=")
                    .append(zooKeeperNamespace)
                    .append(";sasl.qop=")
                    .append(sasl_qop)
                    .append(";auth=")
                    .append(auth)
                    .append(";principal=")
                    .append(principal)
                    .append(";user.principal=")
                    .append(USER_NAME)
                    .append(";user.keytab=")
                    .append(USER_KEYTAB_FILE)
                    .append(";");
        } else {
            /* 普通模式 */
            strBuilder
                    .append(";serviceDiscoveryMode=")
                    .append(serviceDiscoveryMode)
                    .append(";zooKeeperNamespace=")
                    .append(zooKeeperNamespace)
                    .append(";auth=none");
        }
        hiveURL = strBuilder.toString();
        logger.info("【hiveURL】{}", hiveURL);

        // 加载Hive JDBC驱动
        Class.forName(HIVE_DRIVER);
    }

    /**
     * hive查询
     *
     * @param sqls
     */
    public void query(String[] sqls) {
        Connection connection = null;
        try {
            // 获取JDBC连接
            // 如果使用的是普通模式，那么第二个参数需要填写正确的用户名，否则会以匿名用户(anonymous)登录
            connection = DriverManager.getConnection(hiveURL, "", "");
            // 执行查询语句
            for (String sql : sqls) {
                logger.info("即将执行的hive-SQL：{}", sql);
                execDML(connection, sql);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }
}
