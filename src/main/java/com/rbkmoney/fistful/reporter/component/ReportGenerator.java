package com.rbkmoney.fistful.reporter.component;

import com.rbkmoney.fistful.reporter.domain.enums.ReportStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.service.FileInfoService;
import com.rbkmoney.fistful.reporter.service.FileStorageService;
import com.rbkmoney.fistful.reporter.service.ReportService;
import com.rbkmoney.fistful.reporter.service.TemplateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class ReportGenerator {

    private final List<TemplateService> templateServices;
    private final ReportService reportService;
    private final FileInfoService fileInfoService;
    private final FileStorageService fileStorageService;

    public void generateReportFile(Report report) throws RuntimeException {
        try {
            logInfo("Start of report building, ", report);

            List<String> fileDataIds = new ArrayList<>();
            for (TemplateService templateService : templateServices) {
                if (templateService.accept(report.getType())) {
                    logInfo("Create temp report file, template type: " + templateService.getTemplateType() + "; report: ", report);
                    Path reportFile = Files.createTempFile(getReportName(templateService), ".xlsx");
                    try {
                        logInfo("Fill temp report file in with data, template type: " + templateService.getTemplateType() + "; report: ", report);
                        templateService.processReportFileByTemplate(report, Files.newOutputStream(reportFile));

                        logInfo("Save temp report file in file storage, template type: " + templateService.getTemplateType() + "; report: ", report);
                        String fileDataId = fileStorageService.saveFile(reportFile);

                        fileDataIds.add(fileDataId);
                    } finally {
                        logInfo("Delete temp report file, template type: " + templateService.getTemplateType() + "; report: ", report);
                        Files.deleteIfExists(reportFile);
                    }
                }
            }

            finishedReportTask(report, fileDataIds);

            logInfo("Successfully end of report building, ", report);
        } catch (Exception ex) {
            logError("Failed end of report building, ", report, ex);
            throw new RuntimeException(ex);
        }
    }

    private void finishedReportTask(Report report, List<String> fileDataIds) throws StorageException {
        String fileDataIdsLog = fileDataIds.stream()
                .collect(Collectors.joining(", ", "[", "]"));

        logInfo("Save report files information, fileDataIds: " + fileDataIdsLog + "; report: ", report);
        fileInfoService.save(report.getId(), fileDataIds);

        logInfo("Change report status on [created], ", report);
        reportService.changeReportStatus(report, ReportStatus.created);
    }

    private String getReportName(TemplateService templateService) {
        return templateService.getReportPrefixName() + "_report_for_a_period_of_time_" + LocalDateTime.now().toString();
    }

    private void logInfo(String message, Report report) {
        String format = getFormatMessage(message, report);
        log.info(format);
    }

    private void logError(String message, Report report, Exception ex) {
        String format = getFormatMessage(message, report);
        log.error(format, ex);
    }

    private String getFormatMessage(String message, Report report) {
        return String.format(
                message +
                        "reportId='%s', " +
                        "partyId='%s', " +
                        "contractId='%s', " +
                        "fromTime='%s', " +
                        "toTime='%s', " +
                        "createdAt='%s', " +
                        "reportType='%s', " +
                        "status='%s'"
                ,
                report.getId(),
                report.getPartyId(),
                report.getContractId(),
                report.getFromTime().toString(),
                report.getToTime().toString(),
                report.getCreatedAt().toString(),
                report.getType(),
                report.getStatus()
        );
    }
}
