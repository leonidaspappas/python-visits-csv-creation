from flask import Flask,request,jsonify,redirect,render_template
from neo4j import GraphDatabase
import csv

#establish connection
with open("creds/cred.txt") as f1:
    data = csv.reader(f1, delimiter=",")
    for row in data:
        username = row[0]
        pwd = row[1]
        uri = row[2]
print(username, pwd, uri)
driver = GraphDatabase.driver(uri=uri, auth=(username, pwd) )
session = driver.session()
api = Flask(__name__ )
@api.route("/create/<string:name>&<int:id>",methods=["GET","POST"])
def create_node(name, id):
    query1 = """
        create (n:Employee{NAME:$name,ID:$id})
        """
    obj = {"name": name, "id": id}
    try:
        session.run(query1, obj)
        return  f"employee node is created with employee name={name} and id={id}"
    except Exception as e:
        return str(e)
@api.route("/display",methods=["GET","POST"])
def display_node():
    q1 ="""
    match(n:Employee) return n.NAME as NAME ,n.ID as ID
    """
    results = session.run(q1)
    data = results.data()
    return jsonify(data)


if __name__=="__main__":
    api.run(debug=True)