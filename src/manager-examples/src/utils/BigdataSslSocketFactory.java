package utils;

import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.params.HttpParams;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * 使用户设置的TLS版本生效 继承SSLSocketFactory
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class BigdataSslSocketFactory extends SSLSocketFactory {
    private static String[] enabelPro = {"TLSv1.1"};

    public BigdataSslSocketFactory(SSLContext sslContext, X509HostnameVerifier hostnameVerifier,
            String userTLSVersion) {
        super(sslContext, hostnameVerifier);
        enabelPro[0] = userTLSVersion;
    }

    @Override
    public Socket createSocket(HttpParams params) throws IOException {
        Socket result = super.createSocket(params);
        System.out.println("============" + params);
        System.out.println("============" + result);
        System.out.println("============" + result.getPort());
//        try {
//            SSLContext ctx= SSLContext.getInstance("TLSv1.2");
//            ctx.init(null,null,null);
//        } catch (NoSuchAlgorithmException | KeyManagementException e) {
//            e.printStackTrace();
//        }

        // 强转不行，可能需要这样转换
//        SSLSocketFactory sslSf = SSLSocketFactory.getSocketFactory();
//        SSLSocket sslSocket = (SSLSocket) sslSf.createSocket(result, "10.1.2.212",
//            result.getPort(), false);
//        sslSocket.setEnabledProtocols(enabelPro);
//        return sslSocket;

        ((SSLSocket) result).setEnabledProtocols(enabelPro);
        return result;
    }
}
