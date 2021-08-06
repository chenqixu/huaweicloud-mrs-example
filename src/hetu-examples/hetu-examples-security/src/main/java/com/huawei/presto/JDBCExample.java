/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2019. All rights reserved.
 */

package com.huawei.presto;

import java.sql.*;
import java.util.Properties;

/**
 * The example code to connect presto jdbc server and execute sql statement
 *
 * @since 2019-12-01
 */
public class JDBCExample {
    //    private final static String PATH_TO_HETUSERVER_JKS = JDBCExample.class.getClassLoader()
//            .getResource("hetuserver.jks")// 修改点，注释掉，生产没有这个配置文件
//            .getPath();
    private final static String PATH_TO_JAAS_ZK_CONF = JDBCExample.class.getClassLoader()
            .getResource("jaas-zk.conf")
            .getPath();
    private final static String PATH_TO_KRB5_CONF = JDBCExample.class.getClassLoader()
            .getResource("krb5.conf")
            .getPath();
    private final static String PATH_TO_USER_KEYTAB = JDBCExample.class.getClassLoader()
            .getResource("user.keytab")
            .getPath();
    private static Properties properties = new Properties();

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = null;
    private static String AUTH_HOST_NAME = null;

    private static void init() throws ClassNotFoundException {
        System.setProperty("user.timezone", "UTC");
        System.setProperty("java.security.auth.login.config", PATH_TO_JAAS_ZK_CONF);
        System.setProperty("java.security.krb5.conf", PATH_TO_KRB5_CONF);
        properties.setProperty("user", "yz_newland");// 修改点
        properties.setProperty("SSL", "true");
        properties.setProperty("KerberosConfigPath", PATH_TO_KRB5_CONF);
        properties.setProperty("KerberosPrincipal", "yz_newland");// 修改点
        properties.setProperty("KerberosKeytabPath", PATH_TO_USER_KEYTAB);
        properties.setProperty("KerberosRemoteServiceName", "HTTP");
//        properties.setProperty("SSLTrustStorePath", PATH_TO_HETUSERVER_JKS);// 修改点，注释掉，生产没有这个配置文件
        properties.setProperty("tenant", "yz_newland");// 修改点
        properties.setProperty("deploymentMode", "on_yarn");
        properties.setProperty("ZooKeeperAuthType", "kerberos");
        properties.setProperty("ZooKeeperSaslClientConfig", "Client");
        System.out.println("【properties】" + properties);
        // 需要zookeeper认证，修改点
        ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/" + getUserRealm();
        System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        // 加载驱动
        Class.forName("io.prestosql.jdbc.PrestoDriver");
    }

    /**
     * Program entry
     *
     * @param args no need program parameter
     */
    public static void main(String[] args) {
        Connection connection = null;
        ResultSet result = null;
        PreparedStatement statement = null;
        if (args == null || args.length != 1) {
            System.err.println("请输入zk_server!");
            System.exit(-1);
        }
        // 192.168.1.130:24002,192.168.1.131:24002,192.168.1.132:24002
        String zk_server = args[0];
        String url = "jdbc:presto://" + zk_server + "/hive/default?"
                + "serviceDiscoveryMode=zooKeeper&zooKeeperNamespace=hsbroker";
        System.out.println("驱动连接：" + url);
        try {
            init();

            String sql = "show tables";
            connection = DriverManager.getConnection(url, properties);
            statement = connection.prepareStatement(sql.trim());
            result = statement.executeQuery();
            ResultSetMetaData resultMetaData = result.getMetaData();
            Integer colNum = resultMetaData.getColumnCount();
            for (int j = 1; j <= colNum; j++) {
                System.out.print(resultMetaData.getColumnLabel(j) + "\t");
            }
            System.out.println();
            while (result.next()) {
                for (int j = 1; j <= colNum; j++) {
                    System.out.print(result.getString(j) + "\t");
                }
                System.out.println();
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * Get user realm process
     */
    public static String getUserRealm() {
        String serverRealm = System.getProperty("SERVER_REALM");
        if (serverRealm != null && serverRealm != "") {
            AUTH_HOST_NAME = "hadoop." + serverRealm.toLowerCase();
        } else {
            serverRealm = KerberosUtil.getKrb5DomainRealm();
            if (serverRealm != null && serverRealm != "") {
                AUTH_HOST_NAME = "hadoop." + serverRealm.toLowerCase();
            } else {
                AUTH_HOST_NAME = "hadoop";
            }
        }
        return AUTH_HOST_NAME;
    }
}
