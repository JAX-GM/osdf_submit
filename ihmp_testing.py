#!/usr/bin/env python
from __future__ import print_function

import logging, sys, os, time
import importlib
from pprint import pprint
from cutlass import iHMPSession
from cutlass_utils import format_query
# from cutlass_search import retrieve_nodes, query_all_oql

# Log It!
def log_it(logname=os.path.basename(__file__)):
    """log_it setup"""
    curtime = time.strftime("%Y%m%d-%H%M")
    logfile = curtime + logname + '.log'

    loglevel = logging.DEBUG
    logFormat = \
        "%(asctime)s %(levelname)5s: %(name)15s %(funcName)s: %(message)s"
    formatter = logging.Formatter(logFormat)

    logging.basicConfig(format=logFormat)
    l = logging.getLogger(logname)
    l.setLevel(loglevel)

    # root = logging.getLogger()
    # root.setLevel(loglevel)

    # fh = logging.FileHandler(logfile, mode='a')
    # fh.setFormatter(formatter)

    # root.addHandler(fh)
    # l.addHandler(fh)

    return l

def load_string_from_file(filename):
    with open(os.path.join(os.curdir, filename)) as f:
        return f.read().strip()

class auth():
    username = load_string_from_file('auth/username.txt')
    password = load_string_from_file('auth/password.txt')

def dprint(*args):
    """dprint is print with a prefix"""
    pref='  -> '
    print(pref,
          ' '.join([str(arg) for arg in args])
         )

import re

try:
    log = log_it()
    session = iHMPSession(auth.username, auth.password)
    osdf = session.get_osdf()
    info = osdf.get_info()
    print(info)
    # info = session.get_session()
    # info = session.port
    # print(info)
except Exception as e:
    raise e

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def query_all_oql(session, namespace, node_type, query):
    """use oql_query_all_pages for complete sets of results
    [ Requires pre-existing 'iHMPSession'! ]
    """
    cumulative = session.get_osdf().oql_query_all_pages(namespace, query)
    results = cumulative['results']
    nodes = {r['id']:r
             for r in results
             if r['node_type'] == node_type}
    count = len(nodes)
    return (nodes, count)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def query_all_samples(query):
    """use oql_query_all_pages for complete sets of results"""
    #TODO: refactor without presuming pre-existing session
    from cutlass.Sample import Sample
    sn = session
    ns = Sample.namespace
    nt = 'sample'
    (meta, count) = query_all_oql(sn,ns,nt,query)
    return (meta, count)

def query_all_visits(query):
    """use oql_query_all_pages for complete sets of results"""
    #TODO: refactor without presuming pre-existing session
    from cutlass.Visit import Visit
    return query_all_oql(session, Visit.namespace, 'visit', query)

def query_all_wgsdna(query):
    """use oql_query_all_pages for complete sets of results"""
    from cutlass.WgsDnaPrep import WgsDnaPrep
    return query_all_oql(session, WgsDnaPrep.namespace, 'wgs_dna_prep', query)

def query_all_wgsraw(query):
    """use oql_query_all_pages for complete sets of results"""
    from cutlass.WgsRawSeqSet import WgsRawSeqSet
    return query_all_oql(session, WgsRawSeqSet.namespace, 'wgs_raw_seq_set', query)

def query_all_16sdna(query):
    """use oql_query_all_pages for complete sets of results"""
    from cutlass.SixteenSDnaPrep import SixteenSDnaPrep
    return query_all_oql(session, SixteenSDnaPrep.namespace, '16s_dna_prep', query)

def query_all_16sraw(query):
    """use oql_query_all_pages for complete sets of results"""
    from cutlass.SixteenSRawSeqSet import SixteenSRawSeqSet
    return query_all_oql(session, SixteenSRawSeqSet.namespace, '16s_raw_seq_set', query)

def query_all_16strimmed(query):
    """use oql_query_all_pages for complete sets of results"""
    from cutlass.SixteenSTrimmedSeqSet import SixteenSTrimmedSeqSet
    return query_all_oql(session, SixteenSTrimmedSeqSet.namespace, '16s_trimmed_seq_set', query)

def query_all_micrornaraw(query):
    """use oql_query_all_pages for complete sets of results"""
    from cutlass.MicrobTranscriptomicsRawSeqSet import MicrobTranscriptomicsRawSeqSet
    return query_all_oql(session, MicrobTranscriptomicsRawSeqSet.namespace, 'microb_transcriptomics_raw_seq_set', query)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def SubjectSearch():
    print('\n____Subject Search____')
    from cutlass.Subject import Subject
    q = format_query("prediabetes", field="tags")
    s = Subject.search(q)

    if len(s):
        # dprint('query: ',q)
        dprint('count: ',len(s))

# SubjectSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def VisitSearch():
    print('\n____Visit Search____')
    from cutlass.Visit import Visit

    q = format_query("prediabetes", field="tags")
    # node_id = '932d8fbc70ae8f856028b3f67c327c1f'
    # q = format_query(node_id, field="_id")

    id_field = 'visit_id'
    (s,c) = query_all_visits(q)
    dprint('query: ',q)
    if len(s):
        dprint('count: ',c)
        print(','.join([id_field,'id']))
        for id in s:
            id_value = s[id]['meta'][id_field]
            print(','.join([id_value,id]))
        ids = [k for k in s]
        return ids

# VisitSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_sample_children(node_id):
    """get all child (dnaPrep) node records for given sample id"""
    print('\n____Sample Kids')
    from cutlass.Sample import Sample
    search_field = 'name'
    q = format_query(node_id, field="_id")
    # q = '"prediabetes"[tags]'
    dprint('query: ',q)
    (s,c) = query_all_samples(q)
    dprint('count: ',c)
    if len(s):
        # dprint('name''s: ',[ s[k]['name'] for k in s ])
        # dprint('node ids: ', s.keys())
        dprint('name,id: ',[ ','.join([s[k][search_field],k]) for k in s ])
        internal_ids = [s[k][search_field] for k in s]
        # dprint(internal_ids[0])
        node = Sample.load(node_id)
        dprint(node.preps())
        # dprint('id: ', node._id)
        # kids = [prep for prep in node.preps()]

        # preps = node.preps()
        # for prep in preps:

        # preps = [np.next() for np in node.preps()]
        # dprint('prep id 1: ', preps[0])

        for prep in node.preps():
            dprint('kid node: ', str(prep))
            dprint(','.join([prep._prep_id, prep._id, prep._links[0]]))

def SampleSearch():
    print('\n____Sample Search____')
    from cutlass.Sample import Sample

    q = format_query("prediabetes", field="tags")
    id_field = 'name'
    (s,c) = query_all_samples(q)
    dprint('count: ',c)
    if len(s):
        dprint('query: ',q)
        print(','.join([id_field,'id']))
        for id in s:
            id_value = s[id]['meta'][id_field]
            # node_type = s[id]['node_type']
            # dprint(','.join([id_value,id,id['_links']]))
            print(','.join([id_value,id]))
        ids = [k for k in s]
        return ids

# SampleSearch()
# get_sample_children('d57eb430d669de8329be1769d4f52164')

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def SixteenSDnaSearch():
    print('\n____16S DNA Search____')
    q = format_query("prediabetes", field="tags")
    (s,c) = query_all_16sdna(q)

    id_field = 'prep_id'
    dprint('query: ',q)
    dprint('count: ',c)
    if len(s):
        # print(','.join([id_field,'id']))
        print(','.join(['node_type','internal_id','osdf_node_id','parent_node_type','parent_node_id']))
        for id in s:
            node_type = 'sixteensdnaprep'
            id_value = s[id]['meta'][id_field]
            parent = 'sample'
            id_parent = s[id]['linkage']['prepared_from'][0]
            # id_parent = s[id]['linkage'][0][0]
            print(','.join([node_type,id_value,id,parent,id_parent]))
        ids = s.keys()
        return ids

# SixteenSDnaSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def SixteenSRawSearch():
    print('\n____16S RawSeq Search____')
    q = format_query("prediabetes", field="tags")
    (s,c) = query_all_16sraw(q)

    id_field = 'urls'
    dprint('query: ',q)
    dprint('count: ',c)
    if len(s):
        print(','.join(['node_type','internal_id','osdf_node_id','parent_node_type','parent_node_id']))
        for id in s:
            node_type = 'sixteensrawseqset'
            id_value = os.path.basename(s[id]['meta'][id_field][0])
            parent = 'sixteensdnaprep'
            id_parent = s[id]['linkage']['sequenced_from'][0]
            print(','.join([node_type,id_value,id,parent,id_parent]))
        ids = [k for k in s]
        return ids

# SixteenSRawSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def SixteenSTrimmedSearch(q):
    print('\n____16S TrimSeq Search____')
    q = format_query("prediabetes", field="tags")

    (s,c) = query_all_16strimmed(q)

    id_field = 'urls'
    dprint('query: ',q)
    dprint('count: ',c)
    if len(s):
        print(','.join(['node_type','internal_id','osdf_node_id','parent_node_type','parent_node_id']))
        for id in s:
            node_type = 'sixteenstrimmedseqset'
            # id_value = os.path.basename(s[id]['meta'][id_field][0])
            id_value = ' & '.join([os.path.basename(s[id]['meta'][id_field][r-1])
                                   for r in range(len(s[id]['meta'][id_field]))])
            parent = 'sixteensrawseqset'
            id_parent = s[id]['linkage']['computed_from'][0]
            print(','.join([node_type,id_value,id,parent,id_parent]))
        ids = [k for k in s]
        return ids

# SixteenSTrimmedSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def WgsDnaSearch():
    """search for node info in the iHMPSession"""
    print('\n____WGS DNA Search____')
    q = format_query("prediabetes", field="tags")

    id_field = 'prep_id'
    (s,c) = query_all_wgsdna(q)
    dprint('query: ',q)
    if len(s):
        # print(','.join([id_field,'id']))
        # for id in s:
        #     id_value = s[id]['meta'][id_field]
        #     node_type = s[id]['node_type']
        #     print(','.join([id_value,id]))
        ids = [id for id in s.keys() if re.search('_M_', s[id]['meta'][id_field])]
        dprint('dna ids: ', len(ids))
        return ids

# WgsDnaSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def WgsRawSearch():
    print('\n____WGS RawSeq Search____')
    from cutlass.WgsRawSeqSet import WgsRawSeqSet
    q = format_query("prediabetes", field="tags")

    id_field = 'urls'
    (s,c) = query_all_wgsraw(q)

    dprint('query: ',q)
    dprint('count: ',c)
    if len(s):
        print(','.join([id_field,'id']))
        for n in s:
            id_value = os.path.basename(s[n]['meta'][id_field][0])
            print(','.join([id_value,n]))
        ids = [id for id in s.keys() if re.search('_M_', id)]
        # ids = [k for k in s]
        return ids

# WgsRawSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def RnaPrepSearch():
    """search for node info in the iHMPSession"""
    print('\n____RNA Search____')
    q = format_query("prediabetes", field="tags")

    id_field = 'prep_id'
    (s,c) = query_all_wgsdna(q)
    dprint('query: ',q)
    # rnas = [r for r in s
    #         if s[r]['meta'][id_field]
    #        ]
    if len(s):
        # dprint(','.join([id_field,'id']))
        # for id in s:
        #     id_value = s[id]['meta'][id_field]
        #     dprint(','.join([id_value,id]))
        ids = [id for id in s.keys() if re.search('_R_', s[id]['meta'][id_field])]
        dprint('rna ids: ', len(ids))
        return ids

# RnaPrepSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def RnaRawSearch():
    print('\n____Rna RawSeq Search____')
    from cutlass.MicrobTranscriptomicsRawSeqSet import MicrobTranscriptomicsRawSeqSet
    q = format_query("prediabetes", field="tags")

    id_field = 'urls'
    (s,c) = query_all_micrornaraw(q)

    dprint('query: ',q)
    dprint('count: ',c)
    if len(s):
        dprint(','.join([id_field,'id']))
        for n in s:
            id_value = os.path.basename(s[n]['meta'][id_field][0])
            dprint(','.join([id_value,n]))
        ids = [k for k in s]
        return ids

# RnaRawSearch()


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_node(session, node_id):
    """return node instance from osdf id"""
    return session.get_osdf().get_node(node_id)


def delete_node_ids(session, node_dict):
    successes = {}
    for id, node_id in node_dict.iteritems():
        node = get_node(session, node_id)
        # success = node.delete()
        #success = #
        session.get_osdf().delete_node(node._id)
        #successes[node_id] = success
        log.info("Deletion of %s:'%s' successful? %s", id, node_id, success)
    return successes


# from osdf_delete_ids import node_ids
# delete_node_ids(session, node_ids)


def delete_nodes():
    # load dict from file:
    from osdf_delete_ids import node_ids
    nodeid = ','.join(node_ids.values())
    # dprint('nodeid: ', nodeid)
    q = format_query(nodeid, patt=",", field="_id", mode="||")
    # q = format_query(nodeid, patt=",", field="prep_id", mode="||")

    # qstrs = ['"{}"[{}]'.format(s,'_id') for s in node_ids.values()]
    # q = '('+ ' || '.join(qstrs) + ')'

    # dprint('q: ', q)

    # from cutlass.SixteenSTrimmedSeqSet import SixteenSTrimmedSeqSet
    # s = SixteenSTrimmedSeqSet.search(q)

    # from cutlass.SixteenSRawSeqSet import SixteenSRawSeqSet
    # s = SixteenSRawSeqSet.search(q)

    # from cutlass.SixteenSDnaPrep import SixteenSDnaPrep
    # s = SixteenSDnaPrep.search(q)

    # from cutlass.MicrobTranscriptomicsRawSeqSet import MicrobTranscriptomicsRawSeqSet
    # s = MicrobTranscriptomicsRawSeqSet.search(q)

    # from cutlass.WgsRawSeqSet import WgsRawSeqSet
    # s = WgsRawSeqSet.search(q)

    # from cutlass.WgsDnaPrep import WgsDnaPrep
    # s = WgsDnaPrep.search(q)


    print('\n____Delete nodes____')
    dprint('results to delete: ', s)
    dprint('to be deleted count: ', len(s))
    status = {}
    for n in s:
        # dprint(n)
        status[n] = n.delete()
    dprint('Action count:', len(status))
    stat_errs = [n for n in status if status[n] == False]
    dprint('Error count:', len(stat_errs))
    dprint('Errors:', stat_errs)

delete_nodes()
# WgsDnaSearch()
# WgsRawSearch()
# RnaPrepSearch()
# SixteenSDnaSearch()
# SixteenSRawSearch()
# SixteenSTrimmedSearch()


if __name__ == '__main__':
    pass

#  vim: set ts=4 sw=4 tw=79 et :
