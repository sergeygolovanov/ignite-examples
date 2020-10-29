package com.examples.ignite.query;

import javax.cache.Cache;

import com.examples.ignite.domain.Organization;
import com.examples.ignite.domain.Person;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;

public class CacheQueryExample {

    private static final String ORG_CACHE = CacheQueryExample.class.getSimpleName() + "Organizations";

    private static final String PERSON_CACHE = CacheQueryExample.class.getSimpleName() + "Persons";

    public static void main(String[] args) {

        try (Ignite ignite = Ignition.start("src/main/resources/example-ignite.xml")) {
            System.out.println(">>> Cache query example started.");

            CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE);
            orgCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            orgCacheCfg.setIndexedTypes(Long.class, Organization.class);

            CacheConfiguration<AffinityKey<Long>, Person> personCacheCfg = new CacheConfiguration<>(PERSON_CACHE);
            personCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            personCacheCfg.setIndexedTypes(AffinityKey.class, Person.class);

            try {
                ignite.getOrCreateCache(orgCacheCfg);
                ignite.getOrCreateCache(personCacheCfg);
                initialize();
                scanQuery();
                textQuery();
            } finally {
                ignite.destroyCache(PERSON_CACHE);
                ignite.destroyCache(ORG_CACHE);
            }

            System.out.println(">>> Cache query example finished.");
        }
    }

    private static void scanQuery() {
        final IgniteCache<BinaryObject, BinaryObject> cache = Ignition.ignite()
                .cache(PERSON_CACHE)
                .withKeepBinary();
        ScanQuery<BinaryObject, BinaryObject> scan = new ScanQuery<>(
                (IgniteBiPredicate<BinaryObject, BinaryObject>) (key, person) -> person.<Double>field("salary") <= 1000
        );
        System.out.println("People with salaries between 0 and 1000 (queried with SCAN query): ");
        cache.query(scan).getAll().forEach(System.out::println);
    }

    private static void textQuery() {
        IgniteCache<Long, Person> cache = Ignition.ignite().cache(PERSON_CACHE);
        QueryCursor<Cache.Entry<Long, Person>> masters = cache.query(new TextQuery<>(Person.class, "Master"));
        QueryCursor<Cache.Entry<Long, Person>> bachelors = cache.query(new TextQuery<>(Person.class, "Bachelor"));
        System.out.println("Following people have 'Master Degree' in their resumes: ");
        masters.getAll().forEach(System.out::println);
        System.out.println("Following people have 'Bachelor Degree' in their resumes: ");
        bachelors.getAll().forEach(System.out::println);
    }

    private static void initialize() {
        final IgniteCache<Long, Organization> orgCache = Ignition.ignite().cache(ORG_CACHE);
        orgCache.clear();

        Organization org1 = new Organization("ApacheIgnite");
        Organization org2 = new Organization("Other");

        orgCache.put(org1.getId(), org1);
        orgCache.put(org2.getId(), org2);

        final IgniteCache<AffinityKey<Long>, Person> colPersonCache = Ignition.ignite().cache(PERSON_CACHE);
        colPersonCache.clear();

        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
        Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
        Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

        colPersonCache.put(p1.key(), p1);
        colPersonCache.put(p2.key(), p2);
        colPersonCache.put(p3.key(), p3);
        colPersonCache.put(p4.key(), p4);
    }

}
