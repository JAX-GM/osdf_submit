#!/usr/bin/env python
""" load Visit into OSDF using info from data file """

import os
import re

from cutlass.Visit import Visit

import settings
from cutlass_utils import \
		load_data, get_parent_node_id, list_tags, format_query, \
		write_csv_headers, values_to_node_dict, write_out_csv, \
		load_node, get_field_header, dump_args, log_it, \
		get_cur_datetime

filename=os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type          = 'visit'
parent_type        = 'subject'
grand_parent_type  = 'study'
great_parent_type  = 'project'
node_tracking_file = settings.node_id_tracking.path

class node_values:
	visit_id = ''
	visit_number = ''
	interval = ''
	clinic_id = ''
	tags = ['rand_subject_id: ','subgroup: ','study: ']


def load(internal_id, search_field):
	"""search for existing node to update, else create new"""

	# node-specific variables:
	NodeTypeName = 'Visit'
	NodeLoadFunc = 'load'

	return load_node(internal_id, search_field, NodeTypeName, NodeLoadFunc)


def validate_record(parent_id, node, record, data_file_name=node_type):
	"""update record fields
	   validate node
	   if valid, save, if not, return false
	"""
	log.info("in validate/save: "+node_type)
	csv_fieldnames = get_field_header(data_file_name)
	write_csv_headers(data_file_name,fieldnames=csv_fieldnames)

	node.visit_id = record['DCC_VISIT_IDS']
	node.visit_number = int(record['visit_id'])
	node.interval = int(record['interval'])
	node.tags = list_tags(
				'rand_subject_id: '+ record['rand_subject_id'],
				'study: prediabetes',
				'subgroup: ' + record['visit_type'],
				)
	log.debug('parent_id: '+str(parent_id))
	node.links = {'by':[parent_id]}

	csv_fieldnames = get_field_header(data_file_name)
	if not node.is_valid():
		write_out_csv(data_file_name+'_invalid_records.csv',
			fieldnames=csv_fieldnames,values=[record,])
		invalidities = node.validate()
		err_str = "Invalid {}!\n\t{}".format(node_type, str(invalidities))
		log.error(err_str)
		# raise Exception(err_str)
	elif node.save():
		write_out_csv(data_file_name+'_submitted.csv',
					  fieldnames=csv_fieldnames,values=[record,])
		return node
	else:
		write_out_csv(data_file_name+'_unsaved_records.csv',
					  fieldnames=csv_fieldnames,values=[record,])
		return False


def submit(data_file, id_tracking_file=node_tracking_file):
	log.info('Starting submission of %ss.', node_type)
	nodes = []
	csv_fieldnames = get_field_header(data_file)
	write_csv_headers(data_file,fieldnames=csv_fieldnames)
	for record in load_data(data_file):
		# if record['consented'] == 'YES' \
		# and record['visit_number'] != 'UNK':
		#if record['visit_number'] != 'UNK':
			# use of 'UNK' = hack workaround for unreconciled visit list
		log.info('\n...next record...')
		try:
			log.debug('data record: '+str(record))

			# node-specific variables:
			load_search_field = 'visit_id'
			internal_id = record['DCC_VISIT_IDS']
			parent_internal_id = record['rand_subject_id']  ##Text ID to find the parent and get back OSDF ID
			grand_parent_internal_id = 'prediabetes'

			parent_id = get_parent_node_id(
				id_tracking_file, parent_type, parent_internal_id)
			# grand_parent_id = get_parent_node_id(
				# id_tracking_file, grand_parent_type, grand_parent_internal_id)

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
						[[node_type.lower(), saved_name, saved.id,
						  parent_type.lower(), parent_internal_id, parent_id,
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
		#else:
		#    write_out_csv(data_file+'_records_no_submit.csv',
		#            fieldnames=record.keys(),values=[record,])
	return nodes


if __name__ == '__main__':
	pass
