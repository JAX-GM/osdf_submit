#!/usr/bin/env python

import sys

from cutlass import iHMPSession

from settings import auth
from settings import NodeRetrievalFiles
from cutlass_utils import log_it, format_query
from cutlass_search import retrieve_nodes, retrieve_query_all

log = log_it('retrieve_osdf_nodes')
log.info('Starting metadata download from OSDF server.')

# load username, password from files
session = iHMPSession(auth.dcc_user, auth.dcc_pass, ssl=False)
log.info('Loaded session: {}'.format(session.get_session()))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Main Actions ~~~~~
def retrieve_node_type(session, node_type):
    data_file_name = NodeRetrievalFiles[node_type]
    qry = '&&'.join([format_query("prediabetes", field="tags"),
                     format_query(node_type, field="node_type")])
    log.debug("query: %s", qry)
    retrieve_query_all(session, qry, data_file_name)


def main():
    """make it happen!"""

    """ Subject node """
    # retrieve_node_type(session, 'subject')
    """ Visit node """
    # retrieve_node_type(session, 'visit')
    """ Sample node """
    retrieve_node_type(session, 'sample')

    """ 16S nodes """
    # retrieve_node_type(session, '16s_dna_prep')
    #retrieve_node_type(session, '16s_raw_seq_set')
    #retrieve_node_type(session, '16s_trimmed_seq_set')

    """ WGS nodes """
    # retrieve_node_type(session, 'wgs_dna_prep')
    # retrieve_node_type(session, 'wgs_raw_seq_set')

    """ RNA nodes """
    # grep wgs_dna_prep set above for '_R_':
    # retrieve_node_type(session, 'microb_transcriptomics_raw_seq_set')

    """ get all node_type's """
    qry = format_query("prediabetes", field="tags")
    # qry = '&&'.join([format_query("prediabetes", field="tags"),
    #                  format_query("932d8fbc70ae8f856028b3f67cc2baab", field="id")])
    # log.debug("query: %s", qry)
    # retrieve_query_all(session, qry)


if __name__ == '__main__':
    sys.exit(main())
