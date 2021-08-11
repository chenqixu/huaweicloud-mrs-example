/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2019. All rights reserved.
 */

package com.huawei.presto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.Properties;

/**
 * The example code to connect presto jdbc server and execute sql statement
 *
 * @since 2019-12-01
 */
public class JDBCExample {
    private final static Logger logger = LoggerFactory.getLogger(JDBCExample.class);
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
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static Properties properties = new Properties();
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
        if (args == null || args.length != 2) {
            System.err.println("请输入zk_server，和要执行的sql文件!");
            System.exit(-1);
        }
        // 192.168.1.130:24002,192.168.1.131:24002,192.168.1.132:24002
        String zk_server = args[0];
        String url = "jdbc:presto://" + zk_server + "/hive/default?"
                + "serviceDiscoveryMode=zooKeeper&zooKeeperNamespace=hsbroker";
        System.out.println("驱动连接：" + url);
        try {
            init();

            // 从文件读取sql
            String[] sql_array = readSqlFromFile(args[1]);
            connection = DriverManager.getConnection(url, properties);

            // 执行查询语句
            for (String sql : sql_array) {
                logger.info("即将执行的SQL：{}", sql);
                execDML(connection, sql);
            }
        } catch (SQLException | ClassNotFoundException | IOException e) {
            e.printStackTrace();
        } finally {
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

    /**
     * Execute DDL Task process
     */
    public static void execDDL(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(sql);
            statement.execute();
        } finally {
            if (null != statement) {
                statement.close();
            }
        }
    }

    /**
     * Execute DML Task process
     */
    public static void execDML(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        ResultSetMetaData resultMetaData = null;

        try {
            // 执行HQL
            statement = connection.prepareStatement(sql);
            resultSet = statement.executeQuery();

            // 输出查询的列名到控制台
            resultMetaData = resultSet.getMetaData();
            int columnCount = resultMetaData.getColumnCount();
            String resultMsg = "";
            for (int i = 1; i <= columnCount; i++) {
                resultMsg += resultMetaData.getColumnLabel(i) + '\t';
            }
            logger.info(resultMsg);

            // 输出查询结果到控制台
            while (resultSet.next()) {
                String result = "";
                for (int i = 1; i <= columnCount; i++) {
                    result += resultSet.getString(i) + '\t';
                }
                logger.info(result);
            }
        } finally {
            if (null != resultSet) {
                resultSet.close();
            }

            if (null != statement) {
                statement.close();
            }
        }
    }

    /**
     * 从文件读取sql
     *
     * @param fileName
     * @return
     * @throws IOException
     */
    public static String[] readSqlFromFile(String fileName) throws IOException {
        // sql从file读取
        StringBuilder sqlSb;
        logger.info("FILE：{}", fileName);
        File file = new File(fileName);
        if (file.exists()) {
            sqlSb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
                String tmp;
                while ((tmp = br.readLine()) != null) {
                    sqlSb.append(tmp);
                }
            }
            logger.info("SQL：{}", sqlSb.toString());
            return sqlSb.toString().split(";");
        }
        return new String[]{};
    }
}
