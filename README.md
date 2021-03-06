# Parallel k-Hops
Parallelizing k-Hops in Neo4j

Instructions
------------ 

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/procedures-1.0-SNAPSHOT.jar`,
that can be copied to the `plugin` directory of your Neo4j instance.

    cp target/procedures-1.0-SNAPSHOT.jar neo4j-enterprise-3.5.8/plugins/.
    

Restart your Neo4j Server. Your new Stored Procedures are available:

    CALL com.maxdemarzi.khops(Node node, Long distance);
    CALL com.maxdemarzi.khops(Node node, Long distance, List<String> relTypes);
    CALL com.maxdemarzi.khops2(Node node, Long distance);
    CALL com.maxdemarzi.khops2(Node node, Long distance, List<String> relTypes);
    CALL com.maxdemarzi.parallel.khops2(Node node, Long distance);
    CALL com.maxdemarzi.parallel.khops2(Node node, Long distance, List<String> relTypes);
    
Call them with:
        
    MATCH (node:MyNode {id:{some_id})) WITH node 
    CALL com.maxdemarzi.khops(node, 3) YIELD value RETURN value;
    
    MATCH (node:MyNode {id:{some_id})) WITH node 
    CALL com.maxdemarzi.khops(node, 3, ['FRIENDS','KNOWS']) YIELD value RETURN value;
    
    MATCH (node:MyNode {id:{some_id})) WITH node 
    CALL com.maxdemarzi.khops2(node, 3) YIELD value RETURN value;
    
    MATCH (node:MyNode {id:{some_id})) WITH node 
    CALL com.maxdemarzi.khops2(node, 3, ['FRIENDS','KNOWS']) YIELD value RETURN value;
    
    MATCH (node:MyNode {id:{some_id})) WITH node 
    CALL com.maxdemarzi.parallel.khops2(node, 3) YIELD value RETURN value;
        
    MATCH (node:MyNode {id:{some_id})) WITH node 
    CALL com.maxdemarzi.parallel.khops2(node, 3, ['FRIENDS','KNOWS']) YIELD value RETURN value;

    
        
    