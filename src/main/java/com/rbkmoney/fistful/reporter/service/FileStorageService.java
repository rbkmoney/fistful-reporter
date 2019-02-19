package com.rbkmoney.fistful.reporter.service;

import java.io.IOException;
import java.nio.file.Path;

public interface FileStorageService {

    String saveFile(Path file) throws IOException, RuntimeException;
}
