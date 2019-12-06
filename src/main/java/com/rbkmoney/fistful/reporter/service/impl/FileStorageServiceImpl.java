package com.rbkmoney.fistful.reporter.service.impl;

import com.rbkmoney.file.storage.FileStorageSrv;
import com.rbkmoney.file.storage.NewFileResult;
import com.rbkmoney.fistful.reporter.config.properties.FileStorageProperties;
import com.rbkmoney.fistful.reporter.exception.FileStorageClientException;
import com.rbkmoney.fistful.reporter.service.FileStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;

@Service
@Slf4j
@RequiredArgsConstructor
public class FileStorageServiceImpl implements FileStorageService {

    private final FileStorageProperties fileStorageProperties;
    private final FileStorageSrv.Iface fileStorageClient;
    private final HttpClient httpClient;

    @Override
    public String saveFile(Path file) {
        String fileName = file.getFileName().toString();

        try {
            log.info("Trying to upload report file");

            NewFileResult result = createNewFile(fileName);

            HttpPut requestPut = httpPut(file, fileName, result);

            HttpResponse response = httpClient.execute(requestPut);

            checkResponse(result.getFileDataId(), response);

            log.info("Report file has been successfully uploaded, fileDataId={}", result.getFileDataId());

            return result.getFileDataId();
        } catch (UnsupportedEncodingException ex) {
            throw new FileStorageClientException(String.format("Error with encoding fileName=%s", fileName), ex);
        } catch (ClientProtocolException ex) {
            throw new FileStorageClientException("Http protocol error", ex);
        } catch (IOException ex) {
            throw new FileStorageClientException("Connection was aborted", ex);
        }
    }

    private NewFileResult createNewFile(String fileName) {
        try {
            return fileStorageClient.createNewFile(Collections.emptyMap(), getTime().toString());
        } catch (TException e) {
            throw new FileStorageClientException(
                    String.format(
                            "Failed to create new, file name='%s'",
                            fileName
                    ),
                    e
            );
        }
    }

    private void checkResponse(String fileDataId, HttpResponse response) {
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            throw new FileStorageClientException(
                    String.format(
                            "Failed to upload report file, fileDataId='%s', response='%s'",
                            fileDataId,
                            response.toString()
                    )
            );
        }
    }

    private HttpPut httpPut(Path file, String fileName, NewFileResult result) throws UnsupportedEncodingException {
        HttpPut requestPut = new HttpPut(result.getUploadUrl());
        requestPut.setHeader("Content-Disposition", "attachment;filename=" + URLEncoder.encode(fileName, StandardCharsets.UTF_8.name()));
        requestPut.setEntity(new FileEntity(file.toFile()));
        return requestPut;
    }

    private Instant getTime() {
        return LocalDateTime.now(fileStorageProperties.getTimeZone())
                .plusMinutes(fileStorageProperties.getUrlLifeTimeDuration())
                .toInstant(ZoneOffset.UTC);
    }
}
