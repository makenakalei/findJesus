import mechanicalsoup as ms
import redis 
import configparser
from elasticsearch import Elasticsearch, helpers

import pandas as pd
import numpy as np

from neo4j import GraphDatabase

print("hello")
class Neo4JConnector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.execute_write(self._create_and_return_greeting, message)
            print(greeting)
    
    def add_links(self, page, links):
        with self.driver.session() as session:
            session.execute_write(self._create_links, page, links)

    @staticmethod
    def _create_links(tx, page, links):
        page = page.decode('utf-8')
        tx.run("CREATE (:Page {url: $page})", page=page)
        for link in links:
            tx.run("MATCH (p:Page) WHERE p.url = $page "
                "CREATE (:Page {url: $link}) -[:LINKS_TO]-> (p)",
                link=link, page=page)
            # tx.run("CREATE (:Page {url: $link}) -[:LINKS_TO]-> (:Page {url: $page})",
            #     link=link, page=page.decode('utf-8'))

    def flush_db(self):
        print("clearing graph db")
        with self.driver.session() as session: 
            session.execute_write(self._flush_db)

    @staticmethod
    def _flush_db(tx):
        tx.run("MATCH (a) -[r]-> () DELETE a, r")
        tx.run("MATCH (a) DELETE a")

# neo4j_connector = Neo4JConnector("bolt://localhost:7689", "neo4j", "databases")
# neo4j_connector.flush_db()
#connector.print_greeting("hello y'all")
#neo4j_connector.add_links(page, links)


config = configparser.ConfigParser()
config.read('example.ini')


es = Elasticsearch(
  "https://d44b99f1bc6c42338be324a116f0314b.us-central1.gcp.cloud.es.io:443",
  api_key="djZCNG1aQUJRdFlxdXVScE1YR2U6OG0tS0trblNSVDJxSjl0Y3E2bkFVUQ=="
)
# username = 'elastic'
# password = os.getenv('ELASTIC_PASSWORD') # Value you set in the environment variable

# es = Elasticsearch(
#     "http://localhost:9200",
#     basic_auth=(username, password)
# )

print(es.info())

def write_to_elastic(es, url, html):
    url = url.decode('utf-8')
    es.index(index='webpages', document={'url': 'url','html': 'html'})

def crawl(browser, r, es, neo4j_connector, url):

    print("downloading page")
    browser.open(url)

    write_to_elastic(es, url, str(browser.page))

    print("parsing for links")
    a_tags = browser.page.find_all("a")
    hrefs = [ a.get("href") for a in a_tags ]

    wikipedia_domain = "https://en.wikipedia.org"
    print('parsing webpage for links')
    links = [ wikipedia_domain + a for a in hrefs if a and a.startswith("/wiki/") ]
    #print(hrefs)

    r.lpush("links", *links)

    # neo4j_connector.add_links(url, links)

browser = ms.StatefulBrowser()

r = redis.Redis()
r.flushall()

start_url= "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", start_url)
print(r.keys("*"))
while link := r.rpop('links'): 
    print(str(link))
    if "Jesus" in str(link):
        break
    crawl(browser, r, es, None, link)


# neo4j_connector.close()