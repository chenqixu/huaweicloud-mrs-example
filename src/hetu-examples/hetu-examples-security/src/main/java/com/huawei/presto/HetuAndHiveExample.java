package com.huawei.presto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * 河图和Hive的示例
 *
 * @author chenqixu
 */
public class HetuAndHiveExample {
    private final static Logger logger = LoggerFactory.getLogger(HetuAndHiveExample.class);

    /**
     * 主函数入口
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            System.err.println("请输入zk_server，和要执行的sql文件!");
            System.exit(-1);
        }
        // 适用于河图，示例：192.168.1.130:24002,192.168.1.131:24002,192.168.1.132:24002
        String zk_server = args[0];
        try {
            // 从文件读取sql
            String[] sql_array = readSqlFromFile(args[1]);

            HetuHelper hetuHelper = new HetuHelper();
            /**
             * 注意1：hetu需要jaas-zk.conf
             * 注意2：hetu需要krb5.conf
             * 注意3：hetu需要user.keytab
             */
            hetuHelper.init(zk_server);
            hetuHelper.query(sql_array);

            HiveHelper hiveHelper = new HiveHelper();
            /**
             * 注意1：hive需要conf/hiveclient.properties
             * 注意2：hive需要conf/user.keytab
             * 注意3：hive需要conf/krb5.conf
             */
            hiveHelper.init();
            hiveHelper.query(sql_array);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 从文件读取sql
     *
     * @param fileName
     * @return
     * @throws IOException
     */
    private static String[] readSqlFromFile(String fileName) throws IOException {
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
