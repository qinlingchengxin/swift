package net.ys;

import net.ys.service.UploadService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SwiftApplicationTests {

    @Resource
    UploadService uploadService;

    @Test
    public void contextLoads() throws IOException {
        uploadService.splitUpload();
    }
}
