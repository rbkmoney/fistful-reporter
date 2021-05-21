package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.fistful.reporter.*;
import com.rbkmoney.fistful.reporter.config.properties.ReportingProperties;
import com.rbkmoney.fistful.reporter.dto.ReportType;
import com.rbkmoney.fistful.reporter.exception.*;
import com.rbkmoney.fistful.reporter.service.FileInfoService;
import com.rbkmoney.fistful.reporter.service.PartyManagementService;
import com.rbkmoney.fistful.reporter.service.ReportService;
import com.rbkmoney.fistful.reporter.util.ThriftUtils;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.woody.api.flow.error.WUnavailableResultException;
import com.rbkmoney.woody.api.flow.error.WUndefinedResultException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class FistfulReportsHandler implements ReportingSrv.Iface {

    private final ReportingProperties reportingProperties;
    private final ReportService reportService;
    private final FileInfoService fileService;
    private final PartyManagementService partyManagementService;

    @Override
    public List<Report> getReports(ReportRequest reportRequest, List<String> reportTypes) throws TException {
        try {
            checkArgs(reportRequest, reportTypes);

            Instant toTime = TypeUtil.stringToInstant(reportRequest.getTimeRange().getToTime());
            Instant fromTime = TypeUtil.stringToInstant(reportRequest.getTimeRange().getFromTime());

            var reportsByRange = reportService.getReportsByRangeNotCancelled(
                    reportRequest.getPartyId(),
                    reportRequest.getContractId(),
                    fromTime,
                    toTime,
                    reportTypes
            );

            if (reportingProperties.getReportsLimit() > 0
                    && reportsByRange.size() > reportingProperties.getReportsLimit()) {
                throw new LimitException(
                        String.format(
                                "Reports size by storage more then limit by properties: storage=%s, properties=%s",
                                reportsByRange.size(),
                                reportingProperties.getReportsLimit()
                        )
                );
            }

            return reportsByRange.stream()
                    .map(report -> ThriftUtils.map(report, fileService.getFileDataIds(report.getId())))
                    .collect(Collectors.toList());
        } catch (InvalidRequestException ex) {
            log.warn("Error with validation request parameters", ex);
            throw new InvalidRequest();
        } catch (LimitException ex) {
            log.warn("Error with report size limit", ex);
            throw new DatasetTooBig();
        } catch (StorageException ex) {
            throw unavailableResultException(ex);
        } catch (Exception ex) {
            throw undefinedResultException("Error when handled \"getReports\"", ex);
        }
    }

    @Override
    public long generateReport(ReportRequest reportRequest, String reportType) throws TException {
        try {
            checkArgs(reportRequest, Collections.singletonList(reportType));

            Instant toTime = TypeUtil.stringToInstant(reportRequest.getTimeRange().getToTime());
            Instant fromTime = TypeUtil.stringToInstant(reportRequest.getTimeRange().getFromTime());

            // проверка на существование в хелгейте
            partyManagementService.getContract(reportRequest.getPartyId(), reportRequest.getContractId());

            return reportService.createReport(
                    reportRequest.getPartyId(),
                    reportRequest.getContractId(),
                    fromTime,
                    toTime,
                    reportType
            );
        } catch (InvalidRequestException ex) {
            log.warn("Error with validation request parameters", ex);
            throw new InvalidRequest();
        } catch (PartyNotFoundException ex) {
            log.warn("Party not found", ex);
            throw new PartyNotFound();
        } catch (ContractNotFoundException ex) {
            log.warn("Contract not found", ex);
            throw new ContractNotFound();
        } catch (StorageException ex) {
            throw unavailableResultException(ex);
        } catch (Exception ex) {
            throw undefinedResultException("Error when handled \"generateReport\"", ex);
        }
    }

    @Override
    public Report getReport(String partyId, String contractId, long reportId) throws TException {
        try {
            return ThriftUtils.map(
                    reportService.getReport(partyId, contractId, reportId),
                    fileService.getFileDataIds(reportId)
            );
        } catch (ReportNotFoundException ex) {
            throw reportNotFound(ex);
        } catch (StorageException ex) {
            throw unavailableResultException(ex);
        } catch (Exception ex) {
            throw undefinedResultException("Error when handled \"getReport\"", ex);
        }
    }

    @Override
    public void cancelReport(String partyId, String contractId, long reportId) throws TException {
        try {
            reportService.cancelReport(partyId, contractId, reportId);
        } catch (ReportNotFoundException ex) {
            throw reportNotFound(ex);
        } catch (StorageException ex) {
            throw unavailableResultException(ex);
        } catch (Exception ex) {
            throw undefinedResultException("Error when handled \"cancelReport\"", ex);
        }
    }

    private ReportNotFound reportNotFound(ReportNotFoundException ex) {
        log.warn("Report not found", ex);
        return new ReportNotFound();
    }

    private WUnavailableResultException unavailableResultException(StorageException e) {
        log.error("Error with storage", e);
        return new WUnavailableResultException("Error with storage", e);
    }

    private WUndefinedResultException undefinedResultException(String msg, Exception e) {
        log.error(msg, e);
        return new WUndefinedResultException(msg, e);
    }

    private void checkArgs(ReportRequest reportRequest, List<String> reportTypes) {
        try {
            Instant fromTime = TypeUtil.stringToInstant(reportRequest.getTimeRange().getFromTime());
            Instant toTime = TypeUtil.stringToInstant(reportRequest.getTimeRange().getToTime());

            if (fromTime.isAfter(toTime)) {
                throw new IllegalArgumentException("fromTime must be less that toTime");
            }

            for (String reportType : reportTypes) {
                checkReportType(reportType);
            }
        } catch (IllegalArgumentException ex) {
            throw new InvalidRequestException("Invalid parameter", ex);
        }
    }

    private void checkReportType(String reportType) {
        if (Arrays.stream(ReportType.values())
                .noneMatch(r -> r.getType().equals(reportType))) {
            throw new IllegalArgumentException("reportType does not exist");
        }
    }
}
