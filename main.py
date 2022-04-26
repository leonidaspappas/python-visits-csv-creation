import csv
import json
from datetime import datetime

import networkx as nx
from meteostat import Stations, Daily
from time import gmtime, strftime, time

start_date = datetime(2010, 1, 1)
end_date = datetime(2010, 12, 31)


def read_visitsTXT():
    with open('data/visits.txt') as f:
        visits = f.readlines()
    ret = [];
    for x in visits:
        if x.strip():
            lineJson = json.loads(x)
            ret.append(lineJson)
    # print(ret)
    return ret


def createVisitsCSV():
    visitsTXT = read_visitsTXT()
    visitsTXT.sort(key=lambda x: x["visit_date:"])
    start_date = visitsTXT[0].get("visit_date:")
    end_date = visitsTXT[len(visitsTXT) - 1].get("visit_date:")

    data = Daily('03772', start_date, end_date)#10637
    data = data.fetch()

    with open('data/visits.csv', 'w', newline='') as visitsOutCSV:
        writer = csv.DictWriter(visitsOutCSV,
                                fieldnames=["id", "visit_id", "visit_date", "engineer_skill_level", "engineer_note",
                                            "outcome", "task_id", "tavg"])
        writer.writeheader()
        i = 0
        for line in visitsTXT:
            writer.writerow({'id': i, 'visit_id': line.get("visit_id"), 'visit_date': line.get("visit_date:"),
                             'engineer_skill_level': line.get("engineer_skill_level").split("LEVEL")[1],
                             'engineer_note': line.get("engineer_note"), 'outcome': line.get("outcome"),
                             'task_id': line.get("task_id"), 'tavg': data.loc[line.get("visit_date:")]['tavg']})
            i = i + 1

    duplicates = []
    with open('data/tasks.csv', 'w', newline='') as visitsOutCSV:
        writer = csv.DictWriter(visitsOutCSV,
                                fieldnames=["id", "task_id", "node_id", "original_reported_date", "node_age",
                                            "node_type", "task_type", "outcome"])
        writer.writeheader()
        i = 0
        for line in reversed(visitsTXT):
            if duplicates.__contains__(line.get("task_id")):
                continue
            else:
                writer.writerow({'id': i, 'task_id': line.get("task_id"), 'node_id': line.get("node_id"),
                                 'original_reported_date': line.get("original_reported_date"),
                                 'node_age': line.get("node_age"), 'node_type': line.get("node_type"),
                                 'task_type': line.get("task_type"), 'outcome': line.get("outcome")})
                i = i + 1
                duplicates.append(line.get("task_id"))

    graphmlFromAdjList()


def graphmlFromAdjList():
    with open('data/network.adjlist') as f:
        network = f.readlines()
    graph = nx.DiGraph()
    with open('data/connections.csv', 'w', newline='') as visitsOutCSV:
        writer = csv.DictWriter(visitsOutCSV,
                                fieldnames=["name", "edge"])
        writer.writeheader()
        for x in network:
            line = x.split()
            focal_node = line[0]
            if len(line) < 2:
                writer.writerow({'name': focal_node, 'edge': ''})
            else:
                for friend in line[1:]:  # loop over the friends
                    graph.add_edge(focal_node, friend)  # add each edge to the graph
                    writer.writerow({'name': focal_node, 'edge': friend})
    # nx.draw(graph)
    # plt.show()
    with open('data/nodes.csv', 'w', newline='') as visitsOutCSV:
        writer = csv.DictWriter(visitsOutCSV,
                                fieldnames=["name"])
        writer.writeheader()
        for n in graph.nodes:
            writer.writerow({'name': n})

    return graph;


if __name__ == '__main__':
    createVisitsCSV()
    print("task finished")
