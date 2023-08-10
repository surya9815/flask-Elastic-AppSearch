#Flask Imports
from flask import Flask, render_template, request

#Elatic Imports
from elasticsearch import Elasticsearch,helpers,ElasticsearchException
from elasticsearch_dsl import Search

#ELASTIC Initialization
host = 'localhost'
port = 9200
scheme = 'http'
es = Elasticsearch([{'host':host,'port':port,'scheme':scheme}])

#Flask Initialization
app = Flask(__name__)

# @app.route("/index")
# def index():
#     # Opening JSON file
#     f = open('movies.json',)
#     documents = []
#     for i in f.readlines():
#         documents.append(i)
    
#     #use Bulk helper
#     data = helpers.bulk(
#         es,
#         documents,
#         index="movies"

#     )
#     return render_template('about.html',data=data)

@app.route("/")
def home():
    data = ""
    es_error = ""
    try:
        data = es.search(index="movies",body={"query":{"match_all":{}}})
    except ElasticsearchException as e:
        es_error = "Configure Cluster credentials in \"config.json\ and Index data with by calling \"/index\" endpoint"
        print(es_error)
    movies_list = []
    if data:
        for i in data['hits']['hits']:
            movies_list.append(i['_source'])

    return render_template('index.html',data=movies_list,es_error = es_error)

if __name__ == "__main__":
    app.run(debug=False)