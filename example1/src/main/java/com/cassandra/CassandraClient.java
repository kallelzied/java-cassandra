package com.cassandra;

import com.cassandra.repository.KeyspaceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cassandra.domain.Person;
import com.cassandra.repository.PersonRepository;

import com.datastax.driver.core.Session;

public class CassandraClient {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraClient.class);

    public static void main(String args[]) {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", null);
        Session session = client.getSession();

        KeyspaceRepository sr = new KeyspaceRepository(session);
        sr.createKeyspace("keyspaceTest", "SimpleStrategy", 1);
        sr.useKeyspace("keyspaceTest");

        PersonRepository br = new PersonRepository(session);
        br.deleteTable("person");
        br.createTable();
        br.alterTablepersons("age", "int");

        br.deleteTable("personByAge");
        br.createTablePersonsByAge();

        Person person = new Person("Zied", "Kallel", 27,"kallelzied@gmail.com");
        br.insertPersonBatch(person);

        br.selectAll().forEach(o -> LOG.info("Age in persons: " + o.getAge()));

        br.selectAllPersonByAge().forEach(o -> LOG.info("Title in personsByTitle: " + o.getAge()));

        br.deletePersonByAge(28);
        br.deleteTable("persons");
        br.deleteTable("personsByTitle");

        sr.deleteKeyspace("keyspaceTest");

        client.close();
    }
}
