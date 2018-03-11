package com.cassandra.repository;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.cassandra.CassandraConnector;
import com.cassandra.domain.Person;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.UUIDs;

import com.cassandra.repository.PersonRepository;
public class PersonRepositoryIntegrationTest {

    private KeyspaceRepository schemaRepository;

    private PersonRepository personRepository;

    private Session session;

    final String KEYSPACE_NAME = "testLibrary";
    final String PERSON = "person";
    final String PERSON_BY_AGE = "personByAge";

    @BeforeClass
    public static void init() throws ConfigurationException, TTransportException, IOException, InterruptedException {
        // Start an embedded Cassandra Server
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(20000L);
    }

    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9142);
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);
        schemaRepository.createKeyspace(KEYSPACE_NAME, "SimpleStrategy", 1);
        schemaRepository.useKeyspace(KEYSPACE_NAME);
        personRepository = new PersonRepository(session);
    }

    @Test
    public void whenCreatingATable_thenCreatedCorrectly() {
        personRepository.deleteTable(PERSON);
        personRepository.createTable();

        ResultSet result = session.execute("SELECT * FROM " + KEYSPACE_NAME + "." + PERSON + ";");

        // Collect all the column names in one list.
        List<String> columnNames = result.getColumnDefinitions().asList().stream().map(cl -> cl.getName()).collect(Collectors.toList());
        assertEquals(columnNames.size(), 4);
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("firstname"));
        assertTrue(columnNames.contains("lastname"));
        assertTrue(columnNames.contains("email"));
    }

    @Test
    public void whenAlteringTable_thenAddedColumnExists() {
        personRepository.deleteTable(PERSON);
        personRepository.createTable();

        personRepository.alterTablepersons("age", "int");

        ResultSet result = session.execute("SELECT * FROM " + KEYSPACE_NAME + "." + PERSON + ";");

        boolean columnExists = result.getColumnDefinitions().asList().stream().anyMatch(cl -> cl.getName().equals("age"));
        assertTrue(columnExists);
    }

    @Test
    public void whenAddingANewPerson_thenPersonExists() {
        personRepository.deleteTable(PERSON_BY_AGE);
        personRepository.createTablePersonsByAge();

        String firstName = "Zied";
        String lastName = "Kallel";
        Person person = new Person(UUIDs.timeBased(), firstName, lastName, 27, "kallelzied@gmail.com");
        personRepository.insertPersonByAge(person);

        Person savedPerson = personRepository.selectByAge(27);
        assertEquals(person.getAge(), savedPerson.getAge());
    }
/*
    @Test
    public void whenAddingANewPersonBatch_ThenPersonAddedInAllTables() {
        // Create table persons
        personRepository.deleteTable(PERSON);
        personRepository.createTable();

        // Create table personsByTitle
        personRepository.deleteTable(PERSON_BY_AGE);
        personRepository.createTablePersonsByAge();

        String title = "Effective Java";
        String author = "Joshua Bloch";
        Person person = new Person(UUIDs.timeBased(), title, author, "Programming");
        personRepository.insertPersonBatch(person);

        List<Person> persons = personRepository.selectAll();

        assertEquals(1, persons.size());
        assertTrue(persons.stream().anyMatch(b -> b.getTitle().equals("Effective Java")));

        List<Person> personsByTitle = personRepository.selectAllPersonByTitle();

        assertEquals(1, personsByTitle.size());
        assertTrue(personsByTitle.stream().anyMatch(b -> b.getTitle().equals("Effective Java")));
    }

    @Test
    public void whenSelectingAll_thenReturnAllRecords() {
        personRepository.deleteTable(PERSON);
        personRepository.createTable();

        Person person = new Person(UUIDs.timeBased(), "Effective Java", "Joshua Bloch", "Programming");
        personRepository.insertperson(person);

        person = new Person(UUIDs.timeBased(), "Clean Code", "Robert C. Martin", "Programming");
        personRepository.insertperson(person);

        List<Person> persons = personRepository.selectAll();

        assertEquals(2, persons.size());
        assertTrue(persons.stream().anyMatch(b -> b.getTitle().equals("Effective Java")));
        assertTrue(persons.stream().anyMatch(b -> b.getTitle().equals("Clean Code")));
    }

    @Test
    public void whenDeletingAPersonByTitle_thenPersonIsDeleted() {
        personRepository.deleteTable(PERSON_BY_AGE);
        personRepository.createTablePersonsByTitle();

        Person person = new Person(UUIDs.timeBased(), "Effective Java", "Joshua Bloch", "Programming");
        personRepository.insertpersonByTitle(person);

        person = new Person(UUIDs.timeBased(), "Clean Code", "Robert C. Martin", "Programming");
        personRepository.insertpersonByTitle(person);

        personRepository.deletepersonByTitle("Clean Code");

        List<Person> persons = personRepository.selectAllPersonByTitle();
        assertEquals(1, persons.size());
        assertTrue(persons.stream().anyMatch(b -> b.getTitle().equals("Effective Java")));
        assertFalse(persons.stream().anyMatch(b -> b.getTitle().equals("Clean Code")));

    }

    @Test(expected = InvalidQueryException.class)
    public void whenDeletingATable_thenUnconfiguredTable() {
        personRepository.createTable();
        personRepository.deleteTable(PERSON);

        session.execute("SELECT * FROM " + KEYSPACE_NAME + "." + PERSON + ";");
    }*/

    @AfterClass
    public static void cleanup() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }
}
