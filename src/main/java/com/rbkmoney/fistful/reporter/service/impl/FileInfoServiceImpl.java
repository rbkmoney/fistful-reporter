package com.rbkmoney.fistful.reporter.service.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.reporter.dao.FileInfoDao;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FileInfo;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.service.FileInfoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class FileInfoServiceImpl implements FileInfoService {

    private final FileInfoDao fileInfoDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<Long> save(long reportId, List<String> fileDataIds) {
        List<Long> fileInfoIds = new ArrayList<>();
        for (String fileDataId : fileDataIds) {
            Long id = save(reportId, fileDataId);
            fileInfoIds.add(id);
        }
        return fileInfoIds;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Long save(long reportId, String fileDataId) {
        try {
            log.info("Trying to save fileInfo, reportId={}, fileDataId={}", reportId, fileDataId);

            FileInfo fileInfo = new FileInfo();
            fileInfo.setReportId(reportId);
            fileInfo.setFileDataId(fileDataId);

            Long id = fileInfoDao.save(fileInfo);

            log.info("FileInfo has been saved, reportId={}, fileDataId={}", reportId, fileDataId);
            return id;
        } catch (DaoException ex) {
            throw new StorageException(
                    String.format("Failed to save file info, reportId=%s, fileDataId=%s", reportId, fileDataId),
                    ex);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<String> getFileDataIds(long reportId) {
        try {
            log.info("Trying to get fileInfo, reportId={}", reportId);

            List<String> fileIds = fileInfoDao.getByReportId(reportId).stream()
                    .map(FileInfo::getFileDataId)
                    .collect(Collectors.toList());

            log.info("FileInfo for report has been found, reportId={}", reportId);

            return fileIds;
        } catch (DaoException ex) {
            throw new StorageException(String.format("Failed to get fileInfo, reportId=%s", reportId), ex);
        }
    }
}
