from dotenv import load_dotenv, find_dotenv
import os

from langchain_community.graphs import Neo4jGraph

import warnings

warnings.filterwarnings("ignore")

if __name__ == '__main__':
    load_dotenv(find_dotenv())

    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
    NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")

    kg = Neo4jGraph(url=NEO4J_URI, username=NEO4J_USERNAME, password=NEO4J_PASSWORD, database=NEO4J_DATABASE)

    # match all nodes in the graph

    cypher = """MATCH (n) RETURN count(n)"""

    result = kg.query(cypher)
    print(result)

    cypher = """MATCH (n) RETURN count(n) as numberOfNodes"""

    result = kg.query(cypher)
    print(result)

    print(f"There are {result[0]}")

    cypher = """MATCH (m:Movie) RETURN count(m) as numberOfMovies"""

    result = kg.query(cypher)
    print(result)

    print(f"There are {result[0]['numberOfMovies']} nodes in this graph")

    cypher = """MATCH (people:Person) RETURN count(people) AS numberOfPeople"""
    result = kg.query(cypher)
    print(result)

    # return node person where name is Tom Hanks
    cypher = """MATCH (tom:Person {name : "Tom Hanks"}) RETURN tom"""
    result = kg.query(cypher)
    print(result)

    cypher = """MATCH (cloudAtlas:Movie {title:"Cloud Atlas"}) RETURN cloudAtlas"""
    result = kg.query(cypher)
    print(result)

    # return node released date and tagline of movie where movie name is cloud atlas
    cypher = """
    MATCH (cloudAtlas:Movie {title:"Cloud Atlas"})
    RETURN cloudAtlas.released,  cloudAtlas.tagline
    """

    result = kg.query(cypher)
    print(result)

    # return title from node movie where released is 1990-2000-
    cypher = """
      MATCH (nineties:Movie) 
      WHERE nineties.released >= 1990 
        AND nineties.released < 2000 
      RETURN nineties.title
      """

    result = kg.query(cypher)
    print(result)

    # pattern matching with multiple nodes
    cypher = """
    MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)
    RETURN actor.name, movie.title LIMIT 10
    """

    result = kg.query(cypher)
    print(result)

    # return all Tom Hanks' s movie title
    cypher = """
      MATCH (tom:Person {name: "Tom Hanks"})-[:ACTED_IN]->(tomHanksMovies:Movie) 
      RETURN tom.name,tomHanksMovies.title
      """
    result = kg.query(cypher)
    print(result)

    # return co-actors in all Tom Hanks' s movie
    cypher = """
      MATCH (tom:Person {name:"Tom Hanks"})-[:ACTED_IN]->(m)<-[:ACTED_IN]-(coActors) 
      RETURN coActors.name, m.title
      """
    result = kg.query(cypher)
    print(result)

    # delete data from the  graph
    cypher = """
    MATCH (emil:Person {name:"Emil Eifrem"})-[actedIn:ACTED_IN]->(movie:Movie)
    RETURN emil.name, movie.title
    """
    result = kg.query(cypher)
    print(result)

    cypher = """
    MATCH (emil:Person {name:"Emil Eifrem"})-[actedIn:ACTED_IN]->(movie:Movie)
    DELETE actedIn
    """
    result = kg.query(cypher)
    print(result)

    # add data to the graph
    cypher = """CREATE (andreas:Person {name:"Andreas"}) RETURN andreas"""
    result = kg.query(cypher)
    print(result)

    # link relationship between two nodes
    cypher = """MATCH (andreas:Person {name:"Andreas"}), (emil:Person {name:"Emil Eifrem"})
    MERGE (andreas)-[hasRelationship:WORKS_WITH]->(emil)
    RETURN andreas, hasRelationship, emil
    """

    result = kg.query(cypher)
    print(result)
