import json
import logging

import networkx as nx
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable


class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_friendship(self, person1_name, person2_name):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_and_return_friendship, person1_name, person2_name)
            for row in result:
                print("Created friendship between: {p1}, {p2}".format(p1=row['p1'], p2=row['p2']))

    def create_nodes(self, graph):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_and_return_nodes, graph)
            for row in result:
                print("Created new node")

    @staticmethod
    def _create_and_return_nodes(tx, graph):
        """
        visits = tasksFromVisitsTXT()
        query = (
            "Create "
        )

        for line in visits:
            taskId = line.get("task_id")
            nodeId = line.get("node_id")
            nodeAge = line.get("node_age")
            nodeType = line.get("node_type")
            taskType = line.get("task_type")
            originalReportedDate = line.get("original_reported_date")
            query = query + "(visit" + str(line.get("task_id")) + str(line.get("visit_id")) + ":Visit { visit:'" + str(
                line.get("visit_id")) + "',visit_date:'" + str(line.get("visit_date:")) + "',engineer_skill_level:'" + str(
                line.get("engineer_skill_level")) + "',engineer_note:'" + str(line.get("engineer_note")) +"'}),"

            query = query + "(task" + str(line.get("task_id")) + ":Task { taskId: '" + str(line.get(
                "task_id")) + "',nodeId: '" + str(line.get("node_id:")) + "',nodeAge: '" + str(line.get(
                "node_age")) + "',nodeType: '" + str(line.get("nodeType")) + "',taskType: '" + str(line.get(
                "task_type")) + "',originalReportedDate: '" + str(line.get("original_reported_date")) + "' }), "

        query = query[:-1]
        print(query + " edwwwwwwwwww")

        text_file = open("data/querytest.txt", "w")
        # write string to file
        text_file.write(query)

        # close file
        text_file.close()
        """
        query = (
            "CREATE "
        )
        i = 0
        for n in graph.nodes:
            query = query + "(" + n + ":Node { name: '" + n + "' }), "
            i = i + 1

        i = 0
        for n in graph.nodes:
            for e in graph.edges(n):
                if i == 13588:
                    print("mphka")
                    query = query + "(" + n + ")-[:connects]->(" + e.__getitem__(1) + "); "
                else:
                    query = query + "(" + n + ")-[:connects]->(" + e.__getitem__(1) + "), "
                i = i + 1
        print("EDWWWWW" + str(i))
        result = tx.run(query)

        """
        # i=0
        # for n in graph.nodes:
        #    for e in graph.edges(n):
        #        query = query + "\n" + "CREATE (p" + str(i) + ")-[:KNOWS]->(" + e.__getitem__(1) + ") "
        #    i = i + 1
        # print("teleiwsa: " + query);
        result = tx.run(query)
        # print("tokana to query");
        """
        try:
            return [{"p1": row["p1"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = (
            "CREATE (p1:Person { name: $person1_name }) "
            "CREATE (p2:Person { name: $person2_name }) "
            "CREATE (p1)-[:KNOWS]->(p2) "
            "RETURN p1, p2"
        )
        result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
        try:
            return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_person(self, person_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_person, person_name)
            for row in result:
                print("Found person: {row}".format(row=row))

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = (
            "MATCH (p:Person) "
            "WHERE p.name = $person_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, person_name=person_name)
        return [row["name"] for row in result]


def graphmlFromAdjList():
    with open('data/network.adjlist') as f:
        network = f.readlines()
    graph = nx.DiGraph()

    for x in network:
        line = x.split()
        if len(line) == 1:
            if line[0] not in graph:
                graph.add_node(line[0])
        else:
            focal_node = line[0]
            for friend in line[1:]:  # loop over the friends
                graph.add_edge(focal_node, friend)  # add each edge to the graph
    # nx.draw(graph)
    # plt.show()

    return graph;


def tasksFromVisitsTXT():
    with open('data/visits.txt') as f:
        visits = f.readlines()
    ret = [{}];
    for x in visits:
        lineJson = json.loads(x)
        ret.append(lineJson)
    # print(ret)
    return ret


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = "neo4j+s://da82bca8.databases.neo4j.io:7687"
    user = "neo4j"
    password = "maTAEXNdbKDovmTrhVm9IRpXFVCI_XW0JW9ui-qmtBE"
    app = App(uri, user, password)
    graph = graphmlFromAdjList()
    app.create_nodes(graph)

    tasks = tasksFromVisitsTXT()
    # for task in tasks:
    #    print(task.get('node_id'))
    # app.create_friendship("Alice", "David")
    # app.find_person("Alice")
    app.close()
