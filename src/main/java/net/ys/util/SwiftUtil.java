package net.ys.util;

import org.apache.commons.io.FileUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SwiftUtil {

    static String storageUrl;
    static Header authToken;
    static long expiresTime;

    static String url;
    static String user;
    static String password;

    static {
        try {
            Properties properties = new Properties();
            properties.load(SwiftUtil.class.getClassLoader().getResourceAsStream("config.properties"));
            url = properties.getProperty("url");
            user = properties.getProperty("user");
            password = properties.getProperty("password");
        } catch (Exception e) {
            throw new ExceptionInInitializerError("load properties error!");
        }
    }

    /**
     * 并且获得url和Token信息
     */
    private static void genUrlAndToken() throws IOException {
        if (storageUrl == null || authToken == null || System.currentTimeMillis() > expiresTime) {
            CloseableHttpClient httpClient = null;
            try {
                httpClient = HttpClients.createDefault();
                HttpGet req = new HttpGet(url);
                req.addHeader("X-Storage-User", user);
                req.addHeader("X-Storage-Pass", password);
                HttpResponse response = httpClient.execute(req);
                storageUrl = response.getFirstHeader("X-Storage-Url").getValue();
                authToken = response.getFirstHeader("X-Auth-Token");
                Header authTokenExpires = response.getFirstHeader("X-Auth-Token-Expires");
                int value = Integer.parseInt(authTokenExpires.getValue());
                expiresTime = System.currentTimeMillis() + value * 1000;
            } finally {
                if (httpClient != null) {
                    httpClient.close();
                }
            }

        }
    }

    /**
     * 创建Container
     *
     * @param containerName
     */
    public static boolean createContainer(String containerName) throws IOException {
        CloseableHttpClient httpClient = null;
        try {
            genUrlAndToken();
            httpClient = HttpClients.createDefault();
            HttpPut hpp = new HttpPut(storageUrl + "/" + containerName);
            hpp.addHeader(authToken);
            HttpResponse response = httpClient.execute(hpp);
            int statusCode = response.getStatusLine().getStatusCode();
            return statusCode < 300;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    /**
     * 上传文件
     *
     * @param containerName 容器
     * @param file          文件
     * @param storeName     存储全名
     * @return 返回ETAG
     * @throws IOException
     */
    public static String upload(String containerName, File file, String storeName) throws IOException {
        CloseableHttpClient httpClient = null;
        try {
            genUrlAndToken();
            httpClient = HttpClients.createDefault();
            HttpPut httpPost = new HttpPut(storageUrl + "/" + containerName + "/" + storeName);
            httpPost.addHeader(authToken);
            httpPost.setEntity(new FileEntity(file));
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode < 300) {
                return response.getFirstHeader("ETAG").getValue();
            }
            return null;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    /**
     * 文件流上传
     *
     * @param containerName 容器
     * @param stream        文件流
     * @param storeName     存储全名
     * @return 返回ETAG
     * @throws IOException
     */
    public static String upload(String containerName, InputStream stream, String storeName) throws IOException {
        CloseableHttpClient httpClient = null;
        try {
            genUrlAndToken();
            httpClient = HttpClients.createDefault();
            HttpPut httpPut = new HttpPut(storageUrl + "/" + containerName + "/" + storeName);
            httpPut.setHeader(authToken);
            httpPut.setEntity(new InputStreamEntity(stream));
            HttpResponse response = httpClient.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode < 300) {
                return response.getFirstHeader("ETAG").getValue();
            }
            return null;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    /**
     * 下载文件
     *
     * @param containerName
     * @param storeName
     * @return 返回文件流
     * @throws IOException
     */
    public static InputStream download(String containerName, String storeName) throws IOException {
        CloseableHttpClient httpClient = null;
        try {
            genUrlAndToken();
            httpClient = HttpClients.createDefault();
            HttpGet httpget = new HttpGet(storageUrl + "/" + containerName + "/" + storeName);
            httpget.addHeader(authToken);
            HttpResponse response = httpClient.execute(httpget);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode < 300) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    InputStream input = entity.getContent();
                    String suffix = storeName.substring(storeName.lastIndexOf("."));
                    File tempFile = File.createTempFile("swift_tmp_", suffix);
                    FileUtils.copyInputStreamToFile(input, tempFile);
                    return new FileInputStream(tempFile);
                }
            }
            return null;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    /**
     * 删除容器(容器内不能有对象)
     *
     * @param containerName
     */
    public static boolean deleteContainer(String containerName) throws IOException {
        CloseableHttpClient httpClient = null;
        try {
            genUrlAndToken();
            httpClient = HttpClients.createDefault();
            HttpDelete httpDelete = new HttpDelete(storageUrl + "/" + containerName);
            httpDelete.addHeader(authToken);
            HttpResponse response = httpClient.execute(httpDelete);
            int code = response.getStatusLine().getStatusCode();
            return code < 300;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    /**
     * 删除文件
     *
     * @param containerName
     * @param storeName
     */
    public static boolean deleteFile(String containerName, String storeName) throws IOException {
        CloseableHttpClient httpClient = null;
        try {
            genUrlAndToken();
            httpClient = HttpClients.createDefault();
            HttpDelete httpDelete = new HttpDelete(storageUrl + "/" + containerName + "/" + storeName);
            httpDelete.addHeader(authToken);
            HttpResponse response = httpClient.execute(httpDelete);
            int code = response.getStatusLine().getStatusCode();
            return code < 300;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    /**
     * 获取容器列表
     */
    public static List<String> getContainers() throws IOException {
        CloseableHttpClient httpClient = null;
        try {
            genUrlAndToken();
            httpClient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(storageUrl);
            httpGet.addHeader(authToken);
            HttpResponse response = httpClient.execute(httpGet);
            int code = response.getStatusLine().getStatusCode();
            if (code < 300) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    InputStream input = entity.getContent();
                    ByteArrayOutputStream aos = new ByteArrayOutputStream();
                    byte bytes[] = new byte[1024];
                    int len;
                    while ((len = input.read(bytes)) > 0) {
                        aos.write(bytes, 0, len);
                    }
                    String result = new String(aos.toByteArray(), "UTF-8");
                    return Arrays.asList(result.split("\n"));
                }
            }
            return null;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    /**
     * 获取容器内对象列表
     *
     * @param containerName 容器名称
     */
    public static List<String> getObjects(String containerName) throws IOException {
        CloseableHttpClient httpClient = null;
        try {
            genUrlAndToken();
            httpClient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(storageUrl + "/" + containerName);
            httpGet.addHeader(authToken);
            HttpResponse response = httpClient.execute(httpGet);
            int code = response.getStatusLine().getStatusCode();
            if (code < 300) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    InputStream input = entity.getContent();
                    ByteArrayOutputStream aos = new ByteArrayOutputStream();
                    byte bytes[] = new byte[1024];
                    int len;
                    while ((len = input.read(bytes)) > 0) {
                        aos.write(bytes, 0, len);
                    }
                    String result = new String(aos.toByteArray(), "UTF-8");
                    return Arrays.asList(result.split("\n"));
                }
            }
            return null;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public static void main(String[] args) throws IOException {

        String container = "zl_files";
        String filePath = "E:/test.zip";
        String desFilePath = "E:/test/" + System.currentTimeMillis() + ".zip";

        createContainer(container);

        String storeName = System.currentTimeMillis() + ".zip";
        System.out.println(storeName);
        upload(container, new File(filePath), storeName);

        InputStream stream = download(container, storeName);
        FileUtils.copyInputStreamToFile(stream, new File(desFilePath));

        List<String> containers = getContainers();
        for (String c : containers) {
            System.out.println(c);
        }

        List<String> objects = getObjects(container);
        for (String object : objects) {
            System.out.println(object);
        }

        deleteFile(container, storeName);

        deleteContainer(container);
    }
}
