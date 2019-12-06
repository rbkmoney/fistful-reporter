package com.rbkmoney.fistful.reporter.dao;

public interface FistfulDao<T> extends EventDao {

    Long save(T object);

    T get(String id);

    void updateNotCurrent(String id);
}
