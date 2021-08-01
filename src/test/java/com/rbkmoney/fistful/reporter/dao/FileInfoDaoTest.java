package com.rbkmoney.fistful.reporter.dao;

import com.rbkmoney.fistful.reporter.config.PostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FileInfo;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.random;
import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.randomListOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

@PostgresqlSpringBootITest
public class FileInfoDaoTest {

    @Autowired
    private FileInfoDao fileInfoDao;

    @Autowired
    private ReportDao reportDao;

    @Test
    public void fileInfoDaoTest() {
        final int size = 4;
        long reportId;
        Report report = random(Report.class);
        reportId = reportDao.save(report);
        List<FileInfo> fileInfos = randomListOf(size, FileInfo.class);
        for (FileInfo fileInfo : fileInfos) {
            fileInfo.setReportId(reportId);
            fileInfoDao.save(fileInfo);

        }
        assertEquals(size, fileInfoDao.getByReportId(reportId).size());
    }
}
