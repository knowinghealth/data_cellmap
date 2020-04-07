import neo4j
import py2neo
import logging

logging.basicConfig(level=logging.INFO)

from biodatagraph.datasources import NcbiGene, BigWordList
from biodatagraph.parser import NcbiGeneParser, BigWordListParser

log = logging.getLogger(__name__)

NEO4J_URL = 'bolt://localhost:7687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'test'
ROOT_DIR = '/Users/mpreusse/Downloads/tmp/covidgraph'

if __name__ == '__main__':

    graph = py2neo.Graph(NEO4J_URL, user=NEO4J_USER, password=NEO4J_PASSWORD)

    # download datasources
    # log.info('Download NCBI Gene')
    # ncbigene = NcbiGene(ROOT_DIR)
    # ncbigene.download()

    log.info('Download Big Word List')
    bigwordlist = BigWordList(ROOT_DIR)
    bigwordlist.download()

    # run parsers
    # log.info("Run NCBI Gene Parser")
    # ncbigene_parser = NcbiGeneParser(ROOT_DIR)
    # ncbigene_parser.run('9606')

    log.info("Run Big Word List Parser")
    bigwordlist_parser = BigWordListParser(ROOT_DIR)
    bigwordlist_parser.run()

    # load data to Neo4j
    # TODO create indices
    for ns in bigwordlist_parser.nodesets:
        ns.create(graph)
