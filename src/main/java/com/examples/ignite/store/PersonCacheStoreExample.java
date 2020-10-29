package com.examples.ignite.store;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.stream.Stream;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.h2.jdbcx.JdbcConnectionPool;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

public class PersonCacheStoreExample {

    private static final String CACHE_NAME = PersonCacheStoreExample.class.getSimpleName() + "." + Person.class.getSimpleName();

    private static final class PersonFactory extends CacheJdbcPojoStoreFactory<Long, Person> {
        @Override
        public CacheJdbcPojoStore<Long, Person> create() {
            setDataSource(JdbcConnectionPool.create("jdbc:h2:~/testdb", "sa", ""));
            return super.create();
        }

    }

    private static CacheConfiguration<Long, Person> cacheConfiguration() {
        CacheConfiguration<Long, Person> cfg = new CacheConfiguration<>(CACHE_NAME);
        PersonFactory storeFactory = new PersonFactory();
        storeFactory.setDialect(new H2Dialect());
        JdbcType jdbcType = new JdbcType();
        jdbcType.setCacheName(CACHE_NAME);
        jdbcType.setDatabaseSchema("PUBLIC");
        jdbcType.setDatabaseTable("person");
        jdbcType.setKeyType("java.lang.Long");
        jdbcType.setKeyFields(new JdbcTypeField(Types.BIGINT, "id", Long.class, "id"));
        jdbcType.setValueType("com.examples.ignite.store.PersonCacheStoreExample$Person");
        jdbcType.setValueFields(
                new JdbcTypeField(Types.BIGINT, "id", Long.class, "id"),
                new JdbcTypeField(Types.VARCHAR, "name", String.class, "name")
        );
        storeFactory.setTypes(jdbcType);
        cfg.setCacheStoreFactory(storeFactory);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);
        return cfg;
    }

    public static void main(String[] args) throws IgniteException, SQLException {
        try (Ignite ignite = Ignition.start("src/main/resources/example-ignite.xml")) {
            try (IgniteCache<Long, Person> cache = ignite.getOrCreateCache(cacheConfiguration())) {
                Stream.of(1L, 2L, 3L, 4L)
                        .forEach(
                                o -> System.out.println(cache.get(o))
                        );
            } finally {
                ignite.destroyCache(CACHE_NAME); // Distributed cache could be removed from cluster only by #destroyCache() call.
            }
        }
    }

    private static IgniteConfiguration getIgniteConfiguration() {
        return new IgniteConfiguration()
                .setClientMode(true)
                .setPeerClassLoadingEnabled(true)
                .setDiscoverySpi(new TcpDiscoverySpi()
                        .setIpFinder(new TcpDiscoveryMulticastIpFinder()
                                .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                        )
                );
    }

    static class Person implements Serializable {

        public Person() {
        }

        @QuerySqlField(index = true)
        public Long id;

        @QuerySqlField
        public String name;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }

    }

}