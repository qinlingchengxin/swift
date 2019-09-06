package net.ys.service;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.ys.bean.SegFile;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
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

    String storageUrl;
    Header authToken;

    public void genUrlAndToken() throws IOException {
        if (storageUrl == null || authToken == null) {
            CloseableHttpClient httpClient = null;
            try {
                httpClient = HttpClients.createDefault();
                HttpGet req = new HttpGet(swiftUrl);
                req.addHeader("X-Storage-User", swiftUser);
                req.addHeader("X-Storage-Pass", swiftPass);
                HttpResponse rsp = httpClient.execute(req);
                storageUrl = rsp.getFirstHeader("X-Storage-Url").getValue();
                authToken = rsp.getFirstHeader("X-Auth-Token");
            } finally {
                if (httpClient != null) {
                    httpClient.close();
                }
            }
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
        CloseableHttpClient httpClient = null;
        try {
            httpClient = HttpClients.createDefault();
            HttpPut httpPut = new HttpPut(storageUrl + "/" + container + "/" + storeName);
            httpPut.setHeader(authToken);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] bytes = new byte[2048];
            int len;
            while ((len = stream.read(bytes)) > 0) {
                bos.write(bytes, 0, len);
            }
            bos.close();

            httpPut.setEntity(new InputStreamEntity(new ByteArrayInputStream(bos.toByteArray())));
            HttpResponse response = httpClient.execute(httpPut);
            int code = response.getStatusLine().getStatusCode();
            if (code < 300) {
                return response.getFirstHeader("Etag").getValue();
            }
            return null;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
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
        CloseableHttpClient httpClient = null;
        try {
            httpClient = HttpClients.createDefault();
            HttpGet httpget = new HttpGet(storageUrl + "/" + container + "/" + fileName);
            httpget.addHeader(authToken);
            HttpResponse response = httpClient.execute(httpget);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                return response.getEntity();
            }
            return null;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
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
        CloseableHttpClient httpClient = null;
        try {
            httpClient = HttpClients.createDefault();
            HttpPut httpPut = new HttpPut(storageUrl + "/" + container + "/" + tempName + "?multipart-manifest=put");
            httpPut.setHeader(authToken);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data.getBytes());
            InputStreamEntity inputStreamEntity = new InputStreamEntity(byteArrayInputStream);
            httpPut.setEntity(inputStreamEntity);
            HttpResponse response = httpClient.execute(httpPut);
            return response.getStatusLine().getStatusCode() < 300;
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
     * @param fileName
     */
    public boolean deleteFile(String containerName, String fileName) throws IOException {
        CloseableHttpClient httpClient = null;
        try {
            genUrlAndToken();
            httpClient = HttpClients.createDefault();
            HttpDelete httpDelete = new HttpDelete(storageUrl + "/" + containerName + "/" + fileName);
            httpDelete.addHeader(authToken);
            HttpResponse response = httpClient.execute(httpDelete);
            return response.getStatusLine().getStatusCode() < 300;
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    public String getContainer() {
        return container;
    }
}

