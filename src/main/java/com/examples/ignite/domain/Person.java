package com.examples.ignite.domain;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;

public class Person implements Serializable {

    private static final AtomicLong ID_GEN = new AtomicLong();

    @QuerySqlField(index = true)
    private Long id;

    @QuerySqlField(index = true)
    private Long orgId;

    @QuerySqlField
    private String firstName;

    @QuerySqlField
    private String lastName;

    @QueryTextField
    private String resume;

    @QuerySqlField(index = true)
    private double salary;

    private transient AffinityKey<Long> key;

    public Person() {
    }

    public Person(Organization org, String firstName, String lastName, double salary, String resume) {
        id = ID_GEN.incrementAndGet();

        orgId = org.getId();

        this.firstName = firstName;
        this.lastName = lastName;
        this.salary = salary;
        this.resume = resume;
    }

    public Person(Long id, Long orgId, String firstName, String lastName, double salary, String resume) {
        this.id = id;
        this.orgId = orgId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.salary = salary;
        this.resume = resume;
    }

    public Person(Long id, String firstName, String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public AffinityKey<Long> key() {
        if (key == null) {
            key = new AffinityKey<>(id, orgId);
        }

        return key;
    }

    public Long getId() {
        return id;
    }

    public Long getOrgId() {
        return orgId;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getResume() {
        return resume;
    }

    public double getSalary() {
        return salary;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", orgId=" + orgId +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", resume='" + resume + '\'' +
                ", salary=" + salary +
                '}';
    }

}
