import neo4j
import os
import py2neo
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger('biodatagraph').setLevel(level=logging.DEBUG)

from biodatagraph.datasources import NcbiGene, BigWordList, Ensembl, Refseq, Uniprot
from biodatagraph.parser import NcbiGeneParser, BigWordListParser, EnsemblEntityParser, EnsemblMappingParser, \
    RefseqEntityParser, RefseqCodesParser, UniprotKnowledgebaseParser

log = logging.getLogger(__name__)

ROOT_DIR = os.getenv('ROOT_DIR', '/download')
GC_NEO4J_URL = os.getenv('GC_NEO4J_URL', 'bolt://localhost:7687')
GC_NEO4J_USER = os.getenv('GC_NEO4J_USER', 'neo4j')
GC_NEO4J_PASSWORD = os.getenv('GC_NEO4J_PASSWORD', 'test')
RUN_MODE = os.getenv('RUN_MODE', 'prod')


def run_parser(parser):
    """
    Run a parser and log.

    :param parser: The parser
    :return: The parser after running
    """
    log.info("Run parser {}".format(parser.__class__.__name__))
    parser.run_with_mounted_arguments()
    log.info(parser.container.nodesets)
    log.info(parser.container.relationshipsets)
    return parser


def create_index(graph, parser_list):
    """
    Create all indexes for the RelationshipSets in a list of Parsers.

    :param graph: Py2neo graph instance
    :param parser_list: List of parsers
    """
    for parser in parser_list:
        for relationshipset in parser.container.relationshipsets:
            relationshipset.create_index(graph)


def create_nodesets(graph, parser_list):
    """
    Create the NodeSets for a list of parsers

    :graph: Py2neo graph instance
    :param parser_list: List of Parsers
    """
    for parser in parser_list:
        log.info("Create nodes for parser {}".format(parser.__class__.__name__))
        for nodeset in parser.container.nodesets:
            nodeset.create(graph)


def create_relationshipsets(graph, parser_list):
    """
    Create the RelationshipSets for a list of parsers

    :graph: Py2neo graph instance
    :param parser_list: List of Parsers
    """
    for parser in parser_list:
        log.info("Create relationships for parser {}".format(parser.__class__.__name__))
        for relset in parser.container.relationshipsets:
            relset.create(graph)


if __name__ == '__main__':

    if RUN_MODE.lower() == 'test':
        log.info("Run tests")

    else:
        graph = py2neo.Graph(GC_NEO4J_URL, user=GC_NEO4J_USER, password=GC_NEO4J_PASSWORD)

        # Download Datasources
        # ====================
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
            uniprot.download()

        # run Parsers
        # ================
        parsers_done = []

        ncbigene_parser = NcbiGeneParser(ROOT_DIR)
        ncbigene_parser.taxid = '9606'
        parsers_done.append(run_parser(ncbigene_parser))

        bigwordlist_parser = BigWordListParser(ROOT_DIR)
        parsers_done.append(run_parser(bigwordlist_parser))

        ensembl_entity_parser = EnsemblEntityParser(ROOT_DIR)
        ensembl_entity_parser.taxid = '9606'
        parsers_done.append(run_parser(ensembl_entity_parser))

        ensembl_mapping_parser = EnsemblMappingParser(ROOT_DIR)
        ensembl_mapping_parser.taxid = '9606'
        parsers_done.append(run_parser(ensembl_mapping_parser))

        refseq_entity_parser = RefseqEntityParser(ROOT_DIR)
        refseq_entity_parser.taxid = '9606'
        parsers_done.append(run_parser(refseq_entity_parser))

        refseq_codes_parser = RefseqCodesParser(ROOT_DIR)
        refseq_codes_parser.taxid = '9606'
        parsers_done.append(run_parser(refseq_codes_parser))

        uniprot_knowledgebase_parser = UniprotKnowledgebaseParser(ROOT_DIR)
        uniprot_knowledgebase_parser.taxid = '9606'
        parsers_done.append(run_parser(uniprot_knowledgebase_parser))

        # Load data
        # ================
        create_index(graph, parsers_done)
        create_nodesets(graph, parsers_done)
        create_relationshipsets(graph, parsers_done)
