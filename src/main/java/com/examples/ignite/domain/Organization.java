package com.examples.ignite.domain;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Organization {

    private static final AtomicLong ID_GEN = new AtomicLong();

    @QuerySqlField(index = true)
    private Long id;

    @QuerySqlField(index = true)
    private String name;

    private Address addr;

    private OrganizationType type;

    private Timestamp lastUpdated;

    public Organization() {
    }

    public Organization(String name) {
        id = ID_GEN.incrementAndGet();
        this.name = name;
    }

    public Organization(long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Organization(String name, Address addr, OrganizationType type, Timestamp lastUpdated) {
        id = ID_GEN.incrementAndGet();
        this.name = name;
        this.addr = addr;
        this.type = type;
        this.lastUpdated = lastUpdated;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Address getAddr() {
        return addr;
    }

    public OrganizationType getType() {
        return type;
    }

    public Timestamp getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public String toString() {
        return "Organization{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", addr=" + addr +
                ", type=" + type +
                ", lastUpdated=" + lastUpdated +
                '}';
    }
}
