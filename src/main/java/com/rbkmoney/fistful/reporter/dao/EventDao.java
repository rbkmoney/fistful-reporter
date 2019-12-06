package com.rbkmoney.fistful.reporter.dao;

import com.rbkmoney.dao.GenericDao;

import java.util.Optional;

public interface EventDao extends GenericDao {

    Optional<Long> getLastEventId();

}
