package com.rbkmoney.fistful.reporter.dao;

import com.rbkmoney.dao.GenericDao;
import com.rbkmoney.fistful.reporter.domain.enums.FistfulCashFlowChangeType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FistfulCashFlow;

import java.util.List;

public interface FistfulCashFlowDao extends GenericDao {

    void save(List<FistfulCashFlow> fistfulCashFlowList);

    List<FistfulCashFlow> getByObjId(Long objId, FistfulCashFlowChangeType changeType);

}
