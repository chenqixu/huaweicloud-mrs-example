package com.huawei.presto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * JDBC帮助类
 *
 * @author chenqixu
 */
public class JDBCHelper {
    private final static Logger logger = LoggerFactory.getLogger(JDBCHelper.class);

    /**
     * Execute DDL Task process
     */
    public void execDDL(Connection connection, String sql) throws SQLException {
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
    public void execDML(Connection connection, String sql) throws SQLException {
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
}
