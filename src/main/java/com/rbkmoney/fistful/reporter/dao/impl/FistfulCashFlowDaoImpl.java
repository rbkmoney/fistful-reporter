package com.rbkmoney.fistful.reporter.dao.impl;

import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.fistful.reporter.dao.FistfulCashFlowDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.enums.FistfulCashFlowChangeType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FistfulCashFlow;
import com.rbkmoney.fistful.reporter.domain.tables.records.FistfulCashFlowRecord;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Condition;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.rbkmoney.fistful.reporter.domain.tables.FistfulCashFlow.FISTFUL_CASH_FLOW;

@Component
public class FistfulCashFlowDaoImpl extends AbstractGenericDao implements FistfulCashFlowDao {

    private final RowMapper<FistfulCashFlow> cashFlowRowMapper;

    public FistfulCashFlowDaoImpl(HikariDataSource dataSource) {
        super(dataSource);
        cashFlowRowMapper = new RecordRowMapper<>(FISTFUL_CASH_FLOW, FistfulCashFlow.class);
    }

    @Override
    public void save(List<FistfulCashFlow> cashFlowList) {
        for (FistfulCashFlow paymentCashFlow : cashFlowList) {
            FistfulCashFlowRecord record = getDslContext().newRecord(FISTFUL_CASH_FLOW, paymentCashFlow);
            Query query = getDslContext().insertInto(FISTFUL_CASH_FLOW).set(record);

            executeOne(query);
        }
    }

    @Override
    public List<FistfulCashFlow> getByObjId(Long objId, FistfulCashFlowChangeType cashFlowChangeType) {
        Condition condition = FISTFUL_CASH_FLOW.OBJ_ID.eq(objId)
                .and(FISTFUL_CASH_FLOW.OBJ_TYPE.eq(cashFlowChangeType));
        Query query = getDslContext().selectFrom(FISTFUL_CASH_FLOW).where(condition);

        return fetch(query, cashFlowRowMapper);
    }
}
