package com.rbkmoney.fistful.reporter.generator;

import com.rbkmoney.fistful.reporter.domain.enums.ReportStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.exception.ReportGeneratorException;
import com.rbkmoney.fistful.reporter.service.FileInfoService;
import com.rbkmoney.fistful.reporter.service.FileStorageService;
import com.rbkmoney.fistful.reporter.service.ReportService;
import com.rbkmoney.fistful.reporter.service.TemplateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
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

    public void generateReportFile(Report report) {
        logInfo("Trying to build report, ", report);

        List<String> fileDataIds = new ArrayList<>();

        List<TemplateService> templates = templateServices.stream()
                .filter(templateService -> templateService.accept(report.getType()))
                .collect(Collectors.toList());

        for (TemplateService templateService : templates) {
            try {
                Path reportFile = Files.createTempFile(getReportName(templateService), ".xlsx");

                try {
                    logInfo("Fill report file, template type: " + templateService.getTemplateType() + "; report: ", report);
                    templateService.processReportFileByTemplate(report, Files.newOutputStream(reportFile));

                    logInfo("Upload report file , template type: " + templateService.getTemplateType() + "; report: ", report);
                    String fileDataId = fileStorageService.saveFile(reportFile);

                    fileDataIds.add(fileDataId);
                } finally {
                    Files.deleteIfExists(reportFile);
                }
            } catch (IOException ex) {
                throw new ReportGeneratorException("File can not be written", ex);
            }
        }

        finishedReportTask(report, fileDataIds);

        logInfo("Report has been successfully built, ", report);
    }

    private void finishedReportTask(Report report, List<String> fileDataIds) {
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
