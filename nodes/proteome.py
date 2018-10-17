#!/usr/bin/env python
""" load Proteome into OSDF using info from data file """

import os
import re

from cutlass.ProteomeNonPride import ProteomeNonPride

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, list_tags, format_query, \
        write_csv_headers, values_to_node_dict, write_out_csv, \
        load_node, get_field_header, dump_args, log_it

filename = os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type          = 'ProteomeNonPride'
parent_type        = 'HostAssayPrep'
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
    pepid_url = []
    tags = []


def load(internal_id, search_field):
    """search for existing node to update, else create new"""

    # node-specific variables:
    NodeTypeName = node_type
    NodeLoadFunc = 'load_proteome_nonpride'

    return load_node(internal_id, search_field, NodeTypeName, NodeLoadFunc)


def validate_record(parent_id, node, record, data_file_name=node_type):
    """update record fields
       validate node
       if valid, save, if not, return false"""
    
    csv_fieldnames = get_field_header(data_file_name)
    write_csv_headers(data_file_name,fieldnames=csv_fieldnames)

    #node.prepared_by   = 'Sara Ahadi'
    #node.sequence_type = 'nucleotide'
    #node.format        = 'mzml'
    #node.format_doc    = 'https://en.wikipedia.org/wiki/'
    #node.exp_length    = 0 #record['exp_length']
    node.raw_url.append(record['DCC_File_Path'])
    #node.size          = int(record['size'])
    #node.tags = list_tags(node.tags,
                          # 'test', # for debug!!
                          #'sample name: '+record['sample_name_id'],
                          #'visit id: '+record['visit_id'],
                          #'subject id: '+record['rand_subject_id'],
                          #'file name: '+ record['sample_name_id'] + 'mxML.gz',
    #                     )
    #node.analyzer = 'TOF(Time of Flight)'
    node.comment = record['sample_name_id']
    node.checksums = {'md5':record['MD5Sum'], 'sha256':record['SHA256']}
    node.data_processing_protocol = 'Targeted Data Independent analysis'
    #node.detector = ''
    #node.exp_description = 'Protein profiling of more than 900 samples from pre-diabetic and diabetic participants and different time points of healthy, viral infection and immunization.'
    #node.instrument_name = ''
    #node.pepid_url.append(record['DCC_File_Path'])
    #node.pepid_url.remove('')
    #node.pride_id = ''
    node.processing_method = 'Targeted Data independent analysis with OpenSwath'
    #node.peak_url = ''
    node.protocol_name = 'SWATH_Proteomics (attached)'
    node.sample_name = 'Plasma'
    node.search_engine = 'ProteinPilot Paragon database search algorithm'
    #node.short_label = ''
    #node.software = 'ProteinPilot 5.0.1, OpenSwath, PyProphet, TRIC'
    node.source = 'DuoSpray Ion Source'
    node.subtype = 'host'
    #node.raw_url = ''
    #node.result_url = ''
    #node.other_url = ''
    node.study = 'prediabetes'
    #node.tags = ()
    node.title = 'T2D Prediabetes'

#Targeted Immunoproteomics

    parent_link = {'derived_from':[parent_id]}
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
            internal_id = record['sample_name_id'] + '.proteome'
            parent_internal_id = record['sample_name_id'] + '.hostassayprep'
            grand_parent_internal_id = record['visit_id']

            parent_id = get_parent_node_id(
                id_tracking_file, parent_type, parent_internal_id)

            node_is_new = False # set to True if newbie
            node = load(internal_id, load_search_field)
            if not getattr(node, load_search_field):
                log.debug('loaded node newbie...')
                node_is_new = True

	    import pdb ; pdb.set_trace()
            saved = validate_record(parent_id, node, record,
                                    data_file_name=data_file)
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
