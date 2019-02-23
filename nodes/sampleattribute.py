#!/usr/bin/env python
""" load SampleAttribute into OSDF using info from data file """

import os
import re

from cutlass.SampleAttribute import SampleAttribute

import settings
from cutlass_utils import \
		load_data, get_parent_node_id, list_tags, format_query, \
		write_csv_headers, values_to_node_dict, write_out_csv, \
		load_node, get_field_header, dump_args, log_it, \
		get_cur_datetime

filename=os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type          = 'SampleAttribute'
parent_type        = 'Sample'
grand_parent_type  = 'visit'
great_parent_type  = 'subject'
node_tracking_file = settings.node_id_tracking.path

class node_values:
	name = ''  # sample_name
	fecalcal = ''

def load(internal_id, search_field):
	"""search for existing node to update, else create new"""

	# node-specific variables:
	NodeTypeName = 'SampleAttribute'
	NodeLoadFunc = 'load_sampleattribute'

	return load_node(internal_id, search_field, NodeTypeName, NodeLoadFunc)


def validate_record(parent_id, node, record, data_file_name=node_type):
	"""update record fields
	   validate node
	   if valid, save, if not, return false
	"""
	log.info("in validate/save: "+node_type)
	csv_fieldnames = get_field_header(data_file_name)

	node.fecalcal = ''
	node.study = 'prediabetes'
    node.subtype = 'prediabetes'
    node.tags = list_tags('')


	parent_link = {'associated_with':[parent_id]}
	log.debug('parent_id: '+str(parent_link))
	node.links = parent_link

	if not node.is_valid():
		invalids = data_file_name[:-4]+'.invalid_records.csv'
		write_csv_headers(invalids, fieldnames=csv_fieldnames)
		write_out_csv(invalids, fieldnames=csv_fieldnames,
					  values=[record,])
		invalidities = node.validate()
		err_str = "Invalid {}!\n\t{}".format(node_type, str(invalidities))
		log.error(err_str)
		# raise Exception(err_str)
	elif node.save():
		submitted = data_file_name[:-4]+'.submitted.csv'
		write_csv_headers(submitted, fieldnames=csv_fieldnames)
		write_out_csv(submitted, fieldnames=csv_fieldnames,
					  values=[record,])
		return node
	else:
		unsaved = data_file_name[:-4]+'.unsaved.csv'
		write_csv_headers(unsaved, fieldnames=csv_fieldnames)
		write_out_csv(unsaved, fieldnames=csv_fieldnames,
					  values=[record,])
		return False


def submit(data_file, id_tracking_file=node_tracking_file):
	log.info('Starting submission of %ss.', node_type)
	nodes = []
	csv_fieldnames = get_field_header(data_file)
	for record in load_data(data_file):
		# check not 'unknown' jaxid, not missing visit info
		if len(record['DCC_VISIT_IDS']) > 0:
			log.debug('\n...next record...')
			try:
				log.debug('data record: '+str(record))

				# Node-Specific Variables:
				load_search_field = 'name'
				internal_id = record['sample_name_id']
				parent_internal_id = record['DCC_VISIT_IDS']
				grand_parent_internal_id = record['rand_patient_id']

				parent_id = get_parent_node_id(
					id_tracking_file, parent_type, parent_internal_id)
				log.debug('matched parent_id: %s', parent_id)

				if parent_id:
					node_is_new = False # set to True if newbie
					node = load(internal_id, load_search_field)
					if not getattr(node, load_search_field):
						log.debug('loaded node newbie...')
						node_is_new = True
						
					saved = validate_record(parent_id, node, record,
											data_file_name=data_file)
					if saved:
						header = settings.node_id_tracking.id_fields
						saved_name = getattr(saved, load_search_field)
						vals = values_to_node_dict(
							[[node_type.lower(),saved_name,saved.id,
							  parent_type.lower(),parent_internal_id,parent_id,
							  get_cur_datetime()]],
							header
							)
						nodes.append(vals)
						if node_is_new:
							write_out_csv(id_tracking_file,
								  fieldnames=get_field_header(id_tracking_file),
								  values=vals)
				else:
					log.error('No parent_id found for %s', parent_internal_id)

			except Exception, e:
				log.exception(e)
				raise e
		else:
			write_out_csv(data_file+'_records_no_submit.csv',
						  fieldnames=record.keys(), values=[record,])
	return nodes


# if __name__ == '__main__':
	# pass
