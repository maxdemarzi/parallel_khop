package com.maxdemarzi;

import org.junit.jupiter.api.*;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;

import static org.assertj.core.api.Assertions.assertThat;

public class KHopsTest {
    private static ServerControls neo4j;

    @BeforeAll
    static void startNeo4j() {
        neo4j = TestServerBuilders.newInProcessBuilder()
                .withProcedure(Procedures.class)
                .withFixture(MODEL_STATEMENT)
                .newServer();
    }

    @AfterAll
    static void stopNeo4j() {
        neo4j.close();
    }

    @Test
    void shouldCountkhops()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "MATCH (node:User{username:'User-1'}) WITH node CALL com.maxdemarzi.khops(node, 4) YIELD value RETURN value" );

            // Then I should get what I expect
            assertThat(result.single().get("value").asInt()).isEqualTo(4);
        }
    }

    @Test
    void shouldCountkhopswithRelTypes()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "MATCH (node:User{username:'User-1'}) WITH node CALL com.maxdemarzi.khops(node, 4, ['KNOWS']) YIELD value RETURN value" );

            // Then I should get what I expect
            assertThat(result.single().get("value").asInt()).isEqualTo(2);
        }
    }

    @Test
    void shouldCountkhops2()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "MATCH (node:User{username:'User-1'}) WITH node CALL com.maxdemarzi.khops2(node, 4) YIELD value RETURN value" );

            // Then I should get what I expect
            assertThat(result.single().get("value").asInt()).isEqualTo(4);
        }
    }

    @Test
    void shouldCountkhops2withRelTypes()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "MATCH (node:User{username:'User-1'}) WITH node CALL com.maxdemarzi.khops2(node, 4, ['KNOWS']) YIELD value RETURN value" );

            // Then I should get what I expect
            assertThat(result.single().get("value").asInt()).isEqualTo(2);
        }
    }

    @Test
    void shouldCountParallelkhops()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "MATCH (node:User{username:'User-1'}) WITH node CALL com.maxdemarzi.parallel.khops2(node, 4) YIELD value RETURN value" );

            // Then I should get what I expect
            assertThat(result.single().get("value").asInt()).isEqualTo(4);
        }
    }

    @Test
    void shouldCountParallelkhopswithRelTypes()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "MATCH (node:User{username:'User-1'}) WITH node CALL com.maxdemarzi.parallel.khops2(node, 4, ['KNOWS']) YIELD value RETURN value" );

            // Then I should get what I expect
            assertThat(result.single().get("value").asInt()).isEqualTo(2);
        }
    }

    private static final String MODEL_STATEMENT =
            "CREATE (n1:User { username:'User-1' })" +
                    "CREATE (n2:User { username:'User-2' })" +
                    "CREATE (n3:User { username:'User-3' })" +
                    "CREATE (n4:User { username:'User-4' })" +
                    "CREATE (n5:User { username:'User-5' })" +
                    "CREATE (n6:User { username:'User-6' })" +
                    "CREATE (n7:User { username:'User-7' })" +
                    "CREATE (n8:User { username:'User-8' })" +
                    "CREATE (n1)-[:KNOWS]->(n2)" +
                    "CREATE (n2)-[:KNOWS]->(n3)" +
                    "CREATE (n2)-[:FRIENDS]->(n1)" +
                    "CREATE (n4)-[:FRIENDS]->(n3)" +
                    "CREATE (n4)-[:FRIENDS]->(n5)" +
                    "CREATE (n6)-[:FRIENDS]->(n5)" +
                    "CREATE (n6)-[:FRIENDS]->(n8)" +
                    "CREATE (n6)-[:FRIENDS]->(n7)";
}
