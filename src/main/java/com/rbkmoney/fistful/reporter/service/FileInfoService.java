package com.rbkmoney.fistful.reporter.service;

import java.util.List;

public interface FileInfoService {

    List<Long> save(long reportId, List<String> fileDataIds);

    Long save(long reportId, String fileDataId);

    List<String> getFileDataIds(long reportId);
}
