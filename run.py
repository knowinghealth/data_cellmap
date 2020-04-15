import neo4j
import os
import py2neo
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger('biodatagraph').setLevel(level=logging.DEBUG)

from biodatagraph.datasources import NcbiGene, BigWordList, Ensembl, Refseq, Uniprot
from biodatagraph.parser import NcbiGeneParser, BigWordListParser

log = logging.getLogger(__name__)

NEO4J_URL = 'bolt://localhost:7687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'test'
ROOT_DIR = '/Users/mpreusse/Downloads/tmp/covidgraph'
RUN_MODE = os.getenv('RUN_MODE', 'prod')

if __name__ == '__main__':

    if RUN_MODE.lower() == 'test':
        log.info("Run tests")

    else:

        graph = py2neo.Graph(NEO4J_URL, user=NEO4J_USER, password=NEO4J_PASSWORD)

        log.info('Download NCBI Gene')

        ncbigene = NcbiGene(ROOT_DIR)
        if not ncbigene.latest_local_instance():
            ncbigene.download()

        log.info('Download Big Word List')
        bigwordlist = BigWordList(ROOT_DIR)
        if not bigwordlist.latest_local_instance():
            bigwordlist.download()

        log.info('Download ENSEMBL')
        ensembl = Ensembl(ROOT_DIR)
        if not ensembl.latest_local_instance():
            ensembl.download(ensembl.latest_remote_version(), taxids=['9606'])

        log.info('Download RefSeq')
        refseq = Refseq(ROOT_DIR)
        if not refseq.latest_local_instance():
            refseq.download(refseq.latest_remote_version())

        log.info('Download Uniprot')
        uniprot = Uniprot(ROOT_DIR)
        if not uniprot.latest_local_instance():
            refseq.download(refseq.latest_remote_version())






        # # run parsers
        # # log.info("Run NCBI Gene Parser")
        # # ncbigene_parser = NcbiGeneParser(ROOT_DIR)
        # # ncbigene_parser.run('9606')
        #
        # log.info("Run Big Word List Parser")
        # bigwordlist_parser = BigWordListParser(ROOT_DIR)
        # bigwordlist_parser.run()
        #
        # # load data to Neo4j
        # # TODO create indices
        # for ns in bigwordlist_parser.nodesets:
        #     ns.create(graph)
