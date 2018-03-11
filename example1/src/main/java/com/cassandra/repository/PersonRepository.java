package com.cassandra.repository;

import java.util.ArrayList;
import java.util.List;

import com.cassandra.domain.Person;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class PersonRepository {

    private static final String TABLE_NAME = "person";

    private static final String TABLE_NAME_BY_AGE = TABLE_NAME + "ByAge";

    private Session session;

    public PersonRepository(Session session) {
        this.session = session;
    }

    /**
     * Creates the persons table.
     */
    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME)
                .append("(")
                .append("id uuid PRIMARY KEY, ")
                .append("firstname text,")
                .append("lastname text,")
                .append("email text);");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Creates the persons table.
     */
    public void createTablePersonsByAge() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME_BY_AGE)
                .append("(").append("id uuid, ")
                .append("age int,")
                .append("PRIMARY KEY (age, id));");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Alters the table persons and adds an extra column.
     */
    public void alterTablepersons(String columnName, String columnType) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(TABLE_NAME)
                .append(" ADD ")
                .append(columnName)
                .append(" ")
                .append(columnType)
                .append(";");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Insert a row in the table persons. 
     * 
     * @param person
     */
    public void insertPerson(Person person) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME)
                .append("(id, firstname, lastname, email, age) ")
                .append("VALUES (")
                .append(person.getId()).append(", '")
                .append(person.getFirstName())
                .append("', '")
                .append(person.getLastName())
                .append("', '")
                .append(person.getEmail())
                .append("', ")
                .append(person.getAge())
                .append(");");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Insert a row in the table personsByAge.
     * @param person
     */
    public void insertPersonByAge(Person person) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME_BY_AGE).append("(age, id) ")
                .append("VALUES (")
                .append(person.getAge())
                .append(", ")
                .append(person.getId())
                .append(");");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Insert a person into two identical tables using a batch query.
     * 
     * @param person
     */
    public void insertPersonBatch(Person person) {
        StringBuilder sb = new StringBuilder("BEGIN BATCH ")
                .append("INSERT INTO ")
                .append(TABLE_NAME)
                .append("(id, firstname, lastname, email, age) ")
                .append("VALUES (")
                .append(person.getId()).append(", '")
                .append(person.getFirstName())
                .append("', '")
                .append(person.getLastName())
                .append("', '")
                .append(person.getEmail())
                .append("', ")
                .append(person.getAge())
                .append(");")
                .append("INSERT INTO ")
                .append(TABLE_NAME_BY_AGE)
                .append("(id, age) ")
                .append("VALUES (")
                .append(person.getId())
                .append(", ")
                .append(person.getAge())
                .append(");")
                .append("APPLY BATCH;");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Select person by id.
     * 
     * @return
     */
    public Person selectByAge(int age) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(TABLE_NAME_BY_AGE)
                .append(" WHERE age = ")
                .append(age)
                .append(";");

        final String query = sb.toString();

        ResultSet rs = session.execute(query);

        List<Person> persons = new ArrayList<Person>();

        for (Row r : rs) {
            Person s = new Person(r.getUUID("id"), null, null, r.getInt("age"), null);
            persons.add(s);
        }

        return persons.get(0);
    }

    /**
     * Select all persons from persons
     * 
     * @return
     */
    public List<Person> selectAll() {
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME);

        final String query = sb.toString();
        ResultSet rs = session.execute(query);

        List<Person> persons = new ArrayList<Person>();

        for (Row r : rs) {
            Person person = new Person(r.getUUID("id"), r.getString("firstname"), r.getString("lastname"), r.getInt("age"), r.getString("email"));
            persons.add(person);
        }
        return persons;
    }

    /**
     * Select all persons from personsByTitle
     * @return
     */
    public List<Person> selectAllPersonByAge() {
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME_BY_AGE);

        final String query = sb.toString();
        ResultSet rs = session.execute(query);

        List<Person> persons = new ArrayList<Person>();

        for (Row r : rs) {
            Person person = new Person(r.getUUID("id"), null, null, r.getInt("age"), null);
            persons.add(person);
        }
        return persons;
    }

    /**
     * Delete a person by title.
     */
    public void deletePersonByAge(int age) {
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(TABLE_NAME_BY_AGE).append(" WHERE age = ").append(age).append(";");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Delete table.
     * 
     * @param tableName the name of the table to delete.
     */
    public void deleteTable(String tableName) {
        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ").append(tableName);

        final String query = sb.toString();
        session.execute(query);
    }
}
