package com.rbkmoney.fistful.reporter.service.impl;

import com.rbkmoney.fistful.reporter.dao.FileInfoDao;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FileInfo;
import com.rbkmoney.fistful.reporter.exception.DaoException;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.service.FileInfoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class FileInfoServiceImpl implements FileInfoService {

    private final FileInfoDao fileInfoDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Long save(long reportId, String fileDataId) throws StorageException {
        try {
            log.info("Trying to save file information, reportId='{}', fileDataId='{}'", reportId, fileDataId);
            FileInfo fileInfo = new FileInfo();
            fileInfo.setReportId(reportId);
            fileInfo.setFileDataId(fileDataId);
            Long id = fileInfoDao.save(fileInfo);
            log.info("File information have been saved, reportId='{}', fileDataId='{}'", reportId, fileDataId);
            return id;
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<String> getFileDataIds(long reportId) throws StorageException {
        try {
            log.info("Trying to get files information, reportId='{}'", reportId);

            List<String> fileIds = fileInfoDao.getByReportId(reportId).stream()
                    .map(FileInfo::getFileDataId)
                    .collect(Collectors.toList());
            log.info("Files information for report have been found, reportId='{}'", reportId);
            return fileIds;
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}