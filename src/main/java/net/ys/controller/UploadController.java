package net.ys.controller;

import net.ys.service.UploadService;
import org.apache.http.HttpEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;

/**
 * User: NMY
 * Date: 2019-9-6
 * Time: 13:48
 */
@Controller
public class UploadController {

    @Resource
    private UploadService uploadService;

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @PostMapping("/upload")
    public String upload(MultipartFile file, Model model) {
        try {
            long start = System.currentTimeMillis();
            String fileName = file.getOriginalFilename();
            String storeName = System.currentTimeMillis() + fileName.substring(fileName.lastIndexOf("."));
            String eTag = uploadService.upload(file.getInputStream(), uploadService.getContainer(), storeName);
            if (eTag != null) {
                long useTime = System.currentTimeMillis() - start;
                model.addAttribute("use_time", useTime);
                model.addAttribute("gen_file_name", storeName);
                model.addAttribute("container", uploadService.getContainer());
                return "success";
            }
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }
        model.addAttribute("error", "upload failed");
        return "failed";
    }

    @GetMapping("/split/upload")
    @ResponseBody
    public String splitUpload() {
        try {
            uploadService.splitUpload();
            return "success";
        } catch (IOException e) {
        }
        return "failed";
    }

    @GetMapping("/download")
    public void download(HttpServletResponse response, String fileName) {
        try {
            HttpEntity entity = uploadService.download(fileName);
            if (entity == null) {
                return;
            }

            response.setCharacterEncoding("utf-8");
            response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
            InputStream is = entity.getContent();
            ServletOutputStream out = response.getOutputStream();
            byte[] bytes = new byte[2048];
            int len;
            while ((len = is.read(bytes)) > 0) {
                out.write(bytes, 0, len);
                out.flush();
            }
            out.close();
        } catch (Exception e) {
        }
    }
}
