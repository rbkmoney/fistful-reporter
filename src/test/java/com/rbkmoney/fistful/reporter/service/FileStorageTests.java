package com.rbkmoney.fistful.reporter.service;

import com.rbkmoney.file.storage.FileStorageSrv;
import com.rbkmoney.fistful.reporter.config.AbstractCephConfig;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.thrift.TException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.Files.newInputStream;
import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;

public class FileStorageTests extends AbstractCephConfig {

    @Autowired
    private FileStorageService fileStorageService;

    @Autowired
    private HttpClient httpClient;

    @Autowired
    private FileStorageSrv.Iface client;

    // не работает локально на маке
    @Test
    public void uploadAndDownloadTestTest() throws URISyntaxException, IOException, TException, InterruptedException {
        Path file = getFileFromResources();
        String fileDataId = fileStorageService.saveFile(file);
        String downloadUrl = client.generateDownloadUrl(fileDataId, generateCurrentTimePlusDay().toString());

        HttpResponse responseGet = httpClient.execute(new HttpGet(downloadUrl));
        InputStream content = responseGet.getEntity().getContent();
        assertEquals(getContent(newInputStream(file)), getContent(content));
    }

    private Path getFileFromResources() throws URISyntaxException {
        ClassLoader classLoader = this.getClass().getClassLoader();

        URL url = requireNonNull(classLoader.getResource("respect"));
        return Paths.get(url.toURI());
    }
}
