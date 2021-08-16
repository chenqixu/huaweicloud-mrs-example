package com.huawei.presto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 河图帮助类
 * <p>
 * 注意1：据说jdk版本需要1.8_242，或者比这个高
 *
 * @author chenqixu
 */
public class HetuHelper extends JDBCHelper {
    private final static Logger logger = LoggerFactory.getLogger(HetuHelper.class);
    private final String PATH_TO_JAAS_ZK_CONF = JDBCExample.class.getClassLoader()
            .getResource("jaas-zk.conf").getPath();
    private final String PATH_TO_KRB5_CONF = JDBCExample.class.getClassLoader()
            .getResource("krb5.conf").getPath();
    private final String PATH_TO_USER_KEYTAB = JDBCExample.class.getClassLoader()
            .getResource("user.keytab").getPath();
    private String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = null;
    private Properties properties = null;
    private String hetuURL = null;

    /**
     * 河图初始化
     *
     * @throws ClassNotFoundException
     */
    public void init(String zk_server) throws ClassNotFoundException {
        properties = new Properties();
        System.setProperty("user.timezone", "UTC");
        System.setProperty("java.security.auth.login.config", PATH_TO_JAAS_ZK_CONF);
        System.setProperty("java.security.krb5.conf", PATH_TO_KRB5_CONF);
        properties.setProperty("user", "yz_newland");// 修改点
        properties.setProperty("SSL", "true");
        properties.setProperty("KerberosConfigPath", PATH_TO_KRB5_CONF);
        properties.setProperty("KerberosPrincipal", "yz_newland");// 修改点
        properties.setProperty("KerberosKeytabPath", PATH_TO_USER_KEYTAB);
        properties.setProperty("KerberosRemoteServiceName", "HTTP");
        properties.setProperty("tenant", "yz_newland");// 修改点
        properties.setProperty("deploymentMode", "on_yarn");
        properties.setProperty("ZooKeeperAuthType", "kerberos");
        properties.setProperty("ZooKeeperSaslClientConfig", "Client");
        logger.info("【properties】{}", properties);
        // 需要zookeeper认证，修改点
        ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/" + KerberosHelper.getUserRealm();
        System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        // 加载驱动
        Class.forName("io.prestosql.jdbc.PrestoDriver");

        hetuURL = "jdbc:presto://" + zk_server + "/hive/default?"
                + "serviceDiscoveryMode=zooKeeper&zooKeeperNamespace=hsbroker";
        logger.info("【驱动连接】{}", hetuURL);
    }

    /**
     * 河图查询
     *
     * @param sqls
     */
    public void query(String[] sqls) {
        Connection connection = null;
        assert properties != null;
        try {
            // 获取连接
            connection = DriverManager.getConnection(hetuURL, properties);
            // 执行查询语句
            for (String sql : sqls) {
                logger.info("即将执行的hetu-SQL：{}", sql);
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
