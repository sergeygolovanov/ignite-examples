package com.examples.ignite.store;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cache.store.spring.CacheSpringStoreSessionListener;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.h2.jdbcx.JdbcConnectionPool;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

public class PersonBinaryCacheStoreExample {

    private static final String CACHE_NAME = PersonBinaryCacheStoreExample.class.getSimpleName() + "." + Person.class.getSimpleName();

    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("src/main/resources/example-ignite.xml")) {
            try (IgniteCache<Long, BinaryObject> cacheBinaryObject = ignite.getOrCreateCache(cacheConfiguration());
                 IgniteCache<Long, BinaryObject> cachePerson = ignite.cache(CACHE_NAME)) {
//                System.out.println("-----------------------------");
//                Person person = cacheBinaryObject.get(1L).deserialize();
//                cacheBinaryObject.query(new ScanQuery<>(null)).forEach(
//                        p -> System.out.println(">> " + p)
//                );
//                Stream.of(1L, 2L, 3L, 4L)
//                        .forEach(
//                                o -> System.out.println(cacheBinaryObject.get(o))
//                        );
                System.out.println("-----------------------------");
                Stream.of(1L, 2L, 3L, 4L)
                        .forEach(
                                o -> System.out.println(cachePerson.get(o))
                        );
                System.out.println("-----------------------------");

            } finally {
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }

    private static CacheConfiguration<Long, BinaryObject> cacheConfiguration() {
        CacheConfiguration<Long, BinaryObject> cacheConfiguration = new CacheConfiguration<>(CACHE_NAME);
        cacheConfiguration.setAtomicityMode(TRANSACTIONAL);
//        FactoryBuilder.factoryOf(CacheExampleBinaryStore.class);
//        cacheConfiguration.setStoreKeepBinary(true);
        cacheConfiguration.setCacheStoreFactory(FactoryBuilder.factoryOf(CacheExampleBinaryStore.class));
        cacheConfiguration.setCacheStoreSessionListenerFactories((Factory<CacheStoreSessionListener>) () -> {
            CacheSpringStoreSessionListener lsnr = new CacheSpringStoreSessionListener();
            lsnr.setDataSource(CacheExampleBinaryStore.DATA_SRC);
            return lsnr;
        });
        cacheConfiguration.setReadThrough(true);
        cacheConfiguration.setWriteThrough(true);
        return cacheConfiguration;
    }

    public static class CacheExampleBinaryStore extends CacheStoreAdapter<Long, BinaryObject> {

        public static final DataSource DATA_SRC = JdbcConnectionPool.create("jdbc:h2:~/testdb", "sa", "");

        @IgniteInstanceResource
        private Ignite ignite;

        public CacheExampleBinaryStore() {
        }

        @Override
        public BinaryObject load(Long key) {
            final IgniteBinary binary = ignite.binary();
            final BinaryObjectBuilder builder = binary.builder("com.examples.ignite.store.PersonBinaryCacheStoreExample$Person");
            try (Connection conn = DATA_SRC.getConnection()) {
                try (PreparedStatement st = conn.prepareStatement("select * from person where id = ?")) {
                    st.setLong(1, key);
                    ResultSet rs = st.executeQuery();
                    if (rs.next()) {
                        builder.setField("id", rs.getLong("id"));
                        builder.setField("name", rs.getString("name"));
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return builder.build();
        }

        @Override
        public void write(Cache.Entry<? extends Long, ? extends BinaryObject> entry) {
            System.out.println(entry);
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
            System.out.println(key);
        }
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