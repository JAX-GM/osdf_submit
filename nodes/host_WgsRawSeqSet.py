#!/usr/bin/env python
""" load mWGS Raw Seq Set into OSDF using info from data file """

import os
import re

from cutlass.HostWgsRawSeqSet import HostWgsRawSeqSet

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, list_tags, format_query, \
        write_csv_headers, values_to_node_dict, write_out_csv, \
        load_node, get_field_header, dump_args, log_it

filename = os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type          = 'HostWgsRawSeqSet'
parent_type        = 'HostSeqPrep'
grand_parent_type  = 'Sample'
great_parent_type  = 'Visit'
great_great1_type  = 'Subject'

node_tracking_file = settings.node_id_tracking.path

class node_values:
    study = ''
    comment = ''
    sequence_type = ''
    seq_model = ''
    format = ''
    format_doc = ''
    exp_length = ''
    local_file = ''
    checksums = ''
    size = ''
    study = ''
    urls = []
    tags = []


def load(internal_id, search_field):
    """search for existing node to update, else create new"""

    # node-specific variables:
    NodeTypeName = node_type
    NodeLoadFunc = 'load_hostWgsRawSeqSet'
    return load_node(internal_id, search_field, NodeTypeName, NodeLoadFunc)


def validate_record(parent_id, node, record, data_file_name=node_type):
    """update record fields
       validate node
       if valid, save, if not, return false
    """
    log.info("in validate/save: "+node_type)
    csv_fieldnames = get_field_header(data_file_name)
    write_csv_headers(data_file_name,fieldnames=csv_fieldnames)

    node.study         = 'prediabetes'
    node.comment       = record['file_name']
    node.sequence_type = 'nucleotide'
    node.seq_model     = record['seq_model']
    node.format        = 'fastq'
    node.format_doc    = 'https://en.wikipedia.org/wiki/FASTQ_format'
    node.exp_length    = 0 #record['exp_length']
    node.local_file    = record['local_file']
    node.checksums     = {'md5':record['md5'], 'sha256':record['sha256']}
    node.size          = int(record['size'])
#    node.urls          = record['local_file']
#    node.tags = list_tags(node.tags,
#                          # 'test', # for debug!!
#                          'sample name: '+record['visit_id'],
#                          'visit id: '+record['visit_id'],
#                          'subject id: '+record['rand_subject_id'],
#                          'file prefix: '+ record['prep_id'],
#                          'file name: '+ record['local_file'],
#                         )
    parent_link = {'sequenced_from':[parent_id]}
    log.debug('parent_id: '+str(parent_link))
    node.links = parent_link
    csv_fieldnames = get_field_header(data_file_name)

    if not node.is_valid():
        write_out_csv(data_file_name+'_invalid_records.csv',
                      fieldnames=csv_fieldnames, values=[record,])
        invalidities = node.validate()
        err_str = "Invalid {}!\n\t{}".format(node_type, str(invalidities))
        log.error(err_str)
        # raise Exception(err_str)
    elif node.save():
        write_out_csv(data_file_name+'_submitted.csv',
                      fieldnames=csv_fieldnames, values=[record,])
        return node
    else:
        write_out_csv(data_file_name+'_unsaved_records.csv',
                      fieldnames=csv_fieldnames, values=[record,])
        return False


def submit(data_file, id_tracking_file=node_tracking_file):
    log.info('Starting submission of %ss.', node_type)
    nodes = []
    csv_fieldnames = get_field_header(data_file)
    write_csv_headers(data_file,fieldnames=csv_fieldnames)
    for record in load_data(data_file):
        log.info('\n...next record...')
        try:
            log.debug('data record: '+str(record))

            # node-specific variables:
            load_search_field = 'comment'
            internal_id = record['prepared_from'] + '.hostrawseqprep'
            parent_internal_id = record['prepared_from']
            grand_parent_internal_id = record['visit_id']
            parent_id = get_parent_node_id(
                id_tracking_file, parent_type, parent_internal_id)

            node_is_new = False # set to True if newbie
            node = load(internal_id, load_search_field)
            if not getattr(node, load_search_field):
                log.debug('loaded node newbie...')
                node_is_new = True

            saved = validate_record(parent_id, node, record, data_file_name=data_file)
            if saved:
                header = settings.node_id_tracking.id_fields
                saved_name = getattr(saved, load_search_field)
                vals = values_to_node_dict(
                    [[node_type.lower(),saved_name,saved.id,
                      parent_type.lower(),parent_internal_id,parent_id]],
                    header
                    )
                nodes.append(vals)
                if node_is_new:
                    write_out_csv(id_tracking_file,
                          fieldnames=get_field_header(id_tracking_file),
                          values=vals)

        except Exception, e:
            log.exception(e)
            raise e
    return nodes


if __name__ == '__main__':
    pass
