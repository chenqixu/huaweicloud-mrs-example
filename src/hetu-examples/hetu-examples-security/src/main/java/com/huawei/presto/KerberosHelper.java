package com.huawei.presto;

/**
 * kerberos认证帮助类
 *
 * @author chenqixu
 */
public class KerberosHelper {

    /**
     * Get user realm process
     */
    public static String getUserRealm() {
        String AUTH_HOST_NAME;
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
