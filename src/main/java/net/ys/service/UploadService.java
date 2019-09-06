package net.ys.service;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.ys.bean.SegFile;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * User: NMY
 * Date: 18-1-11
 */
@Service
public class UploadService {

    @Value("${swift.url}")
    private String swiftUrl;

    @Value("${swift.user}")
    private String swiftUser;

    @Value("${swift.pass}")
    private String swiftPass;

    @Value("${swift.srcFilePath}")
    private String srcFilePath;

    @Value("${swift.desPath}")
    private String desPath;

    @Value("${swift.container}")
    private String container;

    @Value("${swift.segContainer}")
    private String segContainer;

    @Value("${swift.perLen}")
    private int perLen;

    Header storageUrl;
    Header authToken;

    public void genUrlAndToken() throws IOException {
        if (storageUrl == null || authToken == null) {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpGet req = new HttpGet(swiftUrl);
            req.addHeader("X-Storage-User", swiftUser);
            req.addHeader("X-Storage-Pass", swiftPass);
            HttpResponse rsp = httpClient.execute(req);
            storageUrl = rsp.getFirstHeader("X-Storage-Url");
            authToken = rsp.getFirstHeader("X-Auth-Token");
        }
    }

    public String upload(InputStream stream, String container, String storeName) throws IOException {
        genUrlAndToken();
        return uploadFile(stream, container, storeName);
    }

    /**
     * 文件流上传
     *
     * @param stream 文件流
     * @throws IOException
     */
    public String uploadFile(InputStream stream, String container, String storeName) throws IOException {
        HttpPut httpPut = new HttpPut(storageUrl.getValue() + "/" + container + "/" + storeName);

        /*InputStreamEntity inputStreamEntity = new InputStreamEntity(stream);
        httpPut.setEntity(inputStreamEntity);//这种方式容易引起异常*/

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] bytes = new byte[2048];
        int len;
        while ((len = stream.read(bytes)) > 0) {
            bos.write(bytes, 0, len);
        }
        httpPut.setEntity(new InputStreamEntity(new ByteArrayInputStream(bos.toByteArray())));
        bos.close();

        httpPut.setHeader(authToken);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = httpClient.execute(httpPut);
        try {
            int code = response.getStatusLine().getStatusCode();
            if (code < 300) {
                return response.getFirstHeader("Etag").getValue();
            }
        } finally {
            response.close();
        }

        return null;
    }

    public HttpEntity download(String fileName) throws IOException {
        genUrlAndToken();
        return downloadFile(fileName);
    }

    /**
     * 下载文件
     *
     * @param fileName
     */
    public HttpEntity downloadFile(String fileName) throws IOException {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpget = new HttpGet(storageUrl.getValue() + "/" + container + "/" + fileName);
        httpget.addHeader(authToken);
        HttpResponse response = httpClient.execute(httpget);
        if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
            return response.getEntity();
        }

        return null;
    }

    /**
     * 分片上传
     *
     * @throws IOException
     */
    public void splitUpload() throws IOException {
        long start = System.currentTimeMillis();
        System.out.println("splitUpload start:" + start);
        genUrlAndToken();

        String tempName = System.currentTimeMillis() + srcFilePath.substring(srcFilePath.lastIndexOf("."));
        File file = new File(srcFilePath);
        List<SegFile> segFiles = splitFile(file, perLen * 1024 * 1024);//切分并且已经存储

        JSONArray data = new JSONArray();
        for (SegFile segFile : segFiles) {
            File f = new File(desPath + "/" + segFile.getTempName());
            String eTag = upload(new FileInputStream(f), segContainer, segFile.getTempName());
            JSONObject object = new JSONObject();
            object.put("path", segContainer + "/" + segFile.getTempName());
            object.put("etag", eTag);
            object.put("size_bytes", f.length());
            data.add(object);
        }
        merge(tempName, data.toString());

        deleteSegmentFiles(segFiles);//删除分片文件，节省存储
        System.out.println("splitUpload end, use time: " + (System.currentTimeMillis() - start));
    }

    /**
     * 删除分片文件,此操作可以放在mq或者开线程执行
     *
     * @param segFiles
     * @throws IOException
     */
    private void deleteSegmentFiles(List<SegFile> segFiles) throws IOException {
        long start = System.currentTimeMillis();
        System.out.println("deleteSegmentFiles start:" + start);
        for (SegFile file : segFiles) {
            deleteFile(segContainer, file.getTempName());
        }
        System.out.println("deleteSegmentFiles end, use time: " + (System.currentTimeMillis() - start));
    }

    /**
     * 切分文件
     *
     * @param file
     * @param perLen
     * @return
     * @throws IOException
     */
    public List<SegFile> splitFile(File file, int perLen) throws IOException {
        long fileLen = file.length();
        long fileNum = fileLen / perLen + (fileLen % perLen == 0 ? 0 : 1);
        int page;
        long startPoint;
        SegFile segFile;
        List<SegFile> segFiles = new ArrayList<>();
        BigDecimal per = new BigDecimal(perLen + "");
        for (page = 0; page < fileNum - 1; page++) {
            startPoint = per.multiply(new BigDecimal(page)).longValue();
            segFile = new SegFile(UUID.randomUUID().toString(), page, startPoint);
            segFiles.add(segFile);
        }
        startPoint = per.multiply(new BigDecimal(page)).longValue();
        segFile = new SegFile(UUID.randomUUID().toString(), page, startPoint);
        segFiles.add(segFile);

        RandomAccessFile fis = new RandomAccessFile(file, "rw");
        FileOutputStream fos;
        long readSize;
        for (SegFile sFile : segFiles) {
            readSize = 0;
            fos = new FileOutputStream(desPath + "/" + sFile.getTempName());
            fis.seek(sFile.getStartPoint());
            int len;
            byte[] bytes = new byte[1024];
            while ((len = fis.read(bytes)) > 0) {
                fos.write(bytes, 0, len);
                readSize += len;
                if (readSize == perLen) {
                    fos.flush();
                    break;
                }
            }
            fos.close();
        }
        fis.close();

        return segFiles;
    }

    /**
     * 合并文件
     *
     * @param tempName
     * @param data
     * @return
     * @throws IOException
     */
    public boolean merge(String tempName, String data) throws IOException {
        CloseableHttpResponse response = null;
        try {
            HttpPut httpPut = new HttpPut(storageUrl.getValue() + "/" + container + "/" + tempName + "?multipart-manifest=put");
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data.getBytes());
            InputStreamEntity inputStreamEntity = new InputStreamEntity(byteArrayInputStream);
            httpPut.setEntity(inputStreamEntity);
            httpPut.setHeader(authToken);
            CloseableHttpClient httpClient = HttpClients.createDefault();
            response = httpClient.execute(httpPut);
            return response.getStatusLine().getStatusCode() < 300;
        } catch (Exception e) {
        } finally {
            if (response != null) {
                response.close();
            }
        }
        return false;
    }

    /**
     * 删除文件
     *
     * @param containerName
     * @param fileName
     */
    public boolean deleteFile(String containerName, String fileName) throws IOException {
        genUrlAndToken();
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpDelete httpDelete = new HttpDelete(storageUrl.getValue() + "/" + containerName + "/" + fileName);
        httpDelete.addHeader(authToken);
        HttpResponse response = httpClient.execute(httpDelete);
        int resultCode = response.getStatusLine().getStatusCode();
        if (resultCode < 300) {
            /*HttpEntity entity = response.getEntity();//删除文件会将删除的文件流返回客户端
            if (entity != null) {
                InputStream stream = entity.getContent();
            }*/
            return true;
        }
        return false;
    }

    /**
     * 创建Container
     *
     * @param containerName
     */
    public boolean createContainer(String containerName) {
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPut hpp = new HttpPut(storageUrl.getValue() + "/" + containerName);
            hpp.addHeader(authToken);
            CloseableHttpResponse response = httpClient.execute(hpp);
            int code = response.getStatusLine().getStatusCode();
            return code < 300;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return false;
    }

    /**
     * 删除容器(容器内不能有对象)
     *
     * @param containerName
     */
    public boolean deleteContainer(String containerName) {
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpDelete httpDelete = new HttpDelete(storageUrl.getValue() + "/" + containerName);
            httpDelete.addHeader(authToken);
            HttpResponse response = httpClient.execute(httpDelete);
            int code = response.getStatusLine().getStatusCode();
            return code < 300;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return false;
    }

    /**
     * 获取容器列表
     */
    public List<String> getContainers() {
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(storageUrl.getValue());
            httpGet.addHeader(authToken);
            HttpResponse response = httpClient.execute(httpGet);
            int code = response.getStatusLine().getStatusCode();
            if (code < 300) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    InputStream input = entity.getContent();
                    ByteArrayOutputStream aos = new ByteArrayOutputStream();
                    byte b[] = new byte[1024];
                    int j;
                    while ((j = input.read(b)) != -1) {
                        aos.write(b, 0, j);
                    }
                    String result = new String(aos.toByteArray());
                    return Arrays.asList(result.split("\n"));
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    /**
     * 获取容器内对象列表
     *
     * @param containerName 容器名称
     */
    public List<String> getObjects(String containerName) {
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(storageUrl.getValue() + "/" + containerName);
            httpGet.addHeader(authToken);
            HttpResponse response = httpClient.execute(httpGet);
            int code = response.getStatusLine().getStatusCode();
            if (code < 300) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    InputStream input = entity.getContent();
                    ByteArrayOutputStream aos = new ByteArrayOutputStream();
                    byte b[] = new byte[1024];
                    int j;
                    while ((j = input.read(b)) != -1) {
                        aos.write(b, 0, j);
                    }
                    String result = new String(aos.toByteArray());
                    return Arrays.asList(result.split("\n"));
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public String getContainer() {
        return container;
    }
}

