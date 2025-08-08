import hashlib
import tempfile
from urllib.parse import quote

from neo4j import GraphDatabase
import json
import morph_kgc
import os

def __map_to_ttl__(data, mapping_file):
    """
    Map data to Turtle format using Morph-KGC.

    Parameters:
    - data (dict): Input data to map.
    - mapping_file (str): Path to the mapping file.

    Returns:
    - rdflib.Graph: An RDF graph in Turtle format.
    """
    morph_config = "[DataSource1]\nmappings:{mapping_file}\nfile_path: {d_file}"
    with tempfile.NamedTemporaryFile(mode='w', delete=False, dir=".", suffix=".json") as d_file:
        json.dump({k: v for k, v in data.items()}, d_file)
    g_rdflib = morph_kgc.materialize(morph_config.format(mapping_file=mapping_file, d_file=d_file.name))
    os.unlink(d_file.name)
    return g_rdflib

def __transform_to_str__(graph):
    """
    Convert an RDF graph to a Turtle string.

    Parameters:
    - graph (rdflib.Graph): The RDF graph.

    Returns:
    - str: The Turtle-formatted string.
    """
    content = graph.serialize(format="ttl")
    content = content.replace("\\\"", "&apos;")
    content = content.replace("'", "&apos;")
    return content

def map_and_save(data, mapping_file, config):
    """
    Map data to RDF and save to Neo4j.

    Parameters:
    - data (dict): Input data to map.
    - mapping_file (str): Path to the mapping file.
    - config (dict): Configuration for Neo4j connection.
    """
    g = __map_to_ttl__(data, mapping_file)
    save_to_neo4j(g, config)

def map_and_print(data, mapping_file, config):
    """
    Map data to RDF and print the result.

    Parameters:
    - data (dict): Input data to map.
    - mapping_file (str): Path to the mapping file.
    - config (dict): Configuration for output (e.g., file path).
    """
    g = __map_to_ttl__(data, mapping_file)
    print_graph(g, config)

def save_to_neo4j(g, config):
    """
    Save an RDF graph to Neo4j.

    Parameters:
    - g (rdflib.Graph): The RDF graph.
    - config (dict): Neo4j connection configuration.
    """
    content = __transform_to_str__(g)
    neo = GraphDatabase.driver(**config['neo4j'])
    with neo.session() as s:
        response = s.run(f"""
        CALL n10s.rdf.import.inline('{content}','Turtle')
        """)
        print(response.single())

def print_graph(g, config):
    """
    Print an RDF graph to stdout or a file.

    Parameters:
    - g (rdflib.Graph): The RDF graph.
    - config (dict): Configuration for output (e.g., 'print_file').
    """
    content = __transform_to_str__(g)
    if 'print_file' in config and config['print_file']:
        with open(config['print_file'], "w") as f:
            f.write(content)
    else:
        print(g.serialize(format="ttl"))

def create_hash(uri):
    """
    Generate a SHA-256 hash for a URI.

    Parameters:
    - uri (str): The URI to hash.

    Returns:
    - str: The hexadecimal hash.
    """
    uri = quote(uri, safe=':/#')
    uri = uri.encode()
    m = hashlib.sha256(uri)
    return m.hexdigest()