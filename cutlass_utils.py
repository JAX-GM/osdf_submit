"""Set of utility functions shared amongst the Stanford/JAXGM OSDF script suite
"""

import os
import csv
import re
import yaml
import logging
import time
import importlib

import cutlass

import settings

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functional ~~~~~
# Log It!
def log_it(logname=os.path.basename(__file__), logdir="logs"):
    """log_it setup"""
    curtime = time.strftime("%Y%m%d-%H%M")
    logfile = '.'.join([curtime, logname, 'log'])
    logfile = os.path.join(logdir, logfile)

    loglevel = logging.DEBUG
    logFormat = \
        "%(asctime)s %(levelname)5s: %(module)15s %(funcName)10s: %(message)s"
    formatter = logging.Formatter(logFormat)

    logging.basicConfig(format=logFormat, filename=logfile, level=loglevel)
    # logging.basicConfig(format=logFormat, level=loglevel)
    l = logging.getLogger(logname)

    ch = logging.StreamHandler()
    ch.setLevel(loglevel)
    ch.setFormatter(formatter)
    l.addHandler(ch)

    fh = logging.FileHandler(logfile, mode='a')
    fh.setLevel(loglevel)
    fh.setFormatter(formatter)
    l.addHandler(fh)

    warnlogfile = '.'.join([curtime, logname, 'WARN', 'log'])
    warnlogfile = os.path.join(logdir, warnlogfile)
    wfh = logging.FileHandler(warnlogfile, mode='a')
    wfh.setLevel(logging.WARNING)
    warn_logFormat = "%(levelname)5s: %(message)s"
    warn_formatter = logging.Formatter(warn_logFormat)
    wfh.setFormatter(formatter)
    l.addHandler(wfh)

    # utils = logging.getLogger(logname)
    # utils.setLevel(loglevel)
    # utils.addHandler(ch)
    # utils.addHandler(fh)

    return l

log = log_it('jaxgm_cutlass_osdf')
# log.setLevel(logging.INFO)
# log = logging
# log = logging.getLogger('utils').addHandler(logging.NullHandler())

# dump_args decorator
# orig from: https://wiki.python.org/moin/PythonDecoratorLibrary#Easy_Dump_of_Function_Arguments
def dump_args(func):
    "This decorator dumps out the arguments passed to a function before calling it"
    argnames = func.func_code.co_varnames[:func.func_code.co_argcount]
    fname = func.func_name

    def func_args(*args,**kwargs):
        log.debug("'{}' args: {}".format(
            fname, ', '.join('%s=%r' % entry
                for entry in zip(argnames,args) + kwargs.items())) )
            # "'"+fname+"' args: "+', '.join(
            # '%s=%r' % entry
            # for entry in zip(argnames,args) + kwargs.items()))
        return func(*args, **kwargs)

    return func_args

# use example:
# @dump_args
# def f1(a,b,c):
#     print a + b + c
#
# f1(1, 2, 3)

def get_cur_datetime():
    """return datetime stamp of NOW"""
    return time.strftime("%Y-%m-%d %H:%M")


def get_field_header(csv_file):
    """returns first row of csv file as list of fieldnames"""
    log.info('Loading fields from {}'.format(csv_file))
    with open(csv_file, 'rU') as csvfh:
        try:
            reader = csv.DictReader(csvfh)
            return list(reader.fieldnames)
        except csv.Error as e:
            log.exception('Reading CSV file %s, line %d: %s',
                    csv_file, reader.line_num, e)


def load_data(csv_file,  delim=',', quotechar='"'):
    """yield row dicts from csv_file using DictReader
    """
    log.info('Loading rows from {}'.format(csv_file))
    with open(csv_file, 'rU') as csvfh:
        reader = csv.DictReader(csvfh, dialect='excel',
                                delimiter=delim, quotechar=quotechar)
        # log.debug('csv dictreader opened')
        try:
            for row in reader:
                # log.debug(row)
                yield row
        except csv.Error as e:
            log.exception('Reading CSV file %s, line %d: %s',
                    csv_file, reader.line_num, e)


def csv_type_sniff(csv_file):
    """find the line/ending type using csv.sniffer"""
    try:
        with open(csv_file, 'rb') as f:
            dialect = csv.Sniffer().sniff(f.read(1024))
            return dialect
    except Exception as e:
        raise e


id_fields = settings.node_id_tracking.id_fields
# sample: subject,69-01,610a491c,study,prediabetes,610a4911a5c

def write_out_csv(csv_file,fieldnames=id_fields,values=[]):
    """write all values in csv format to outfile.
    Values is list of dicts w/ keys matching fieldnames.
    To write header to file, omit `values`
    """
    try:
        with open(csv_file, 'a') as csvout:
            writer = csv.DictWriter(csvout, fieldnames)
            if values:
                log.info('Writing csv to {}'.format(csv_file))
                try:
                    for row in values:
                        # log.debug('Next row {}'.format(str(row)[0:50]+'...'))
                        if isinstance(row, dict):
                            log.debug(row)
                            writer.writerow(row)
                except Exception as e:
                    log.exception('Error writing CSV file %s, %s', csv_file, str(e))
                    raise e
            else:
                log.info('Writing header of fieldnames to {}'.format(csv_file))
                writer.writeheader()
    except IOError as e:
        raise e


def write_csv_headers(base_filename='node_data_file', fieldnames=[]):
    """init other csv files (invalid, unsaved, etc) with fieldname headers"""
    err_file_appends = ['_unsaved_records.csv',
                        '_invalid_records.csv',
                        '_records_no_submit.csv',
                        '_submitted.csv',
                        ]
    [ write_out_csv(
        base_filename+suff,
        fieldnames=fieldnames)
        for suff in err_file_appends
        if not os.path.exists(base_filename+suff) ]


def values_to_node_dict(values=[], keynames=id_fields):
    """pass list of lists of values and list of keys of desired dict
       This converts to list of dicts
    """
    from collections import OrderedDict
    log.debug('In values_to_node_dict')
    final_list = []

    key_dict = OrderedDict()
    for key in keynames:
        key_dict[key] = ''
    # log.debug('key_dict: %s', key_dict)

    for vals in values:
        l = vals
        d = key_dict.copy()
        k = d.keys()
        for x in range(len(d)):
            lx = l[x] if len(l) > x and l[x] is not None else ''
            d[k[x]] = lx
        # log.debug('val_dict: %s', d)
        final_list.append(d)

    return final_list

# values_to_node_dict([['foo','bar','1'],['spam','eggs','2','spoof','filled']])


def get_parent_node_id(id_file_name, node_type, parent_id):
    """ read node ids from csv tracking file
        return first "parent" node matching node_type
    """
    log.debug('--> args: '+ id_file_name +','+ node_type +','+ parent_id)
    try:
        for row in load_data(id_file_name):
            # log.debug('--> checking node row: '+ str(row))
            if re.match(node_type.lower(),row['node_type']):
                # node_ids.append(row.parent_id)
                if re.match(parent_id,row['internal_id']):
                    # log.debug('--> matching node row: '+ str(row))
                    # log.debug('parent type: {}, osdf_node_id: {}'.format(
                    #     node_type,str(row['osdf_node_id'])))
                    return row['osdf_node_id']
                # else:
                    # log.debug('--> no match node row for: '+ str(parent_id))
                    # return None
            # else:
                # log.debug('--> no match node row: '+ str(node_type))
                # return None
    except Exception as e:
        raise e


def get_node_id(id_file_name, node_type, node_id):
    """ read node ids from csv tracking file
        return node id matching node_type
    """
    try:
        for row in load_data(id_file_name):
            if re.match(node_type,row['node_type']):
                if re.match(node_id,row['internal_id']):
                    log.debug('matching, node type: {}, osdf_node_id: {}'.format(
                        node_type,str(row['osdf_node_id'])))
                    return row['osdf_node_id']
    except Exception as e:
        raise e


def get_child_node_ids(id_file_name, node_type, parent_id):
    """ read node ids from csv tracking file
        yield "child" node ids matching node_type
    """
    try:
        for row in load_data(id_file_name):
            if re.match(node_type,row['node_type']):
                if re.match(parent_id,row['internal_id']):
                    log.debug('--> matching node row: '+ str(row))
                    log.debug('parent type: {}, osdf_node_id: {}'.format(
                        node_type,str(row['osdf_node_id'])))
                    return row['osdf_node_id']
    except Exception as e:
        raise e

#TODO: mod node calls to cutlass_utils.load_node; do not need node_load_func
# node = load_node(internal_id, load_search_field, node_type)

def load_node(internal_id, search_field, node_type, node_load_func):
    """search and load nodes, as specified in arguments, else create new"""
    # node-specific variables:
    NodeType = importlib.import_module('cutlass.'+node_type)
    NodeTypeName = getattr(NodeType, node_type)
    NodeLoadFunc = getattr(NodeTypeName, node_load_func)
    NodeSearch = getattr(NodeTypeName, 'search')

    log.info('In load(%s, %s) using node(%s, %s)',
             internal_id, search_field, NodeTypeName, NodeLoadFunc)

    try:
        import pdb ; pdb.set_trace()
        query = format_query(internal_id, field=search_field)
        results = NodeSearch(query)
        log.debug('results: %s', results)
        for node in results:
            getattr_search_field = \
                    format_query(getattr(node, search_field),
                                 field=search_field)
            # log.debug('getattr: %s', getattr(node, search_field))
            log.debug('getattr: %s', getattr_search_field)
            # if internal_id == getattr(node, search_field):
            if query == getattr_search_field:
                log.debug('found node: %s', getattr_search_field)
                return node
        # no match, return new, empty node:
        node = NodeTypeName()
        return node
    except Exception as e:
        raise e


def format_query(strng, patt='[-. ]', field='rand_subj_id', mode='&&'):
    """format OQL query by removing characterset (e.g. '[-\.]')
           1) Split 'strng' on 'patt';
           2) append 'field' text to each piece;
           3) join using 'mode'
           4) return lowercased strng
    """
    mode = ' '+mode.strip()+' ' # spaces between and/or's and strng splits
    strngs = re.split(patt,strng)
    if len(strngs) > 1:
        strngs = ['"{}"[{}]'.format(s,field) for s in strngs]
        #TODO: insert () around first two strngs, plus third, then...
        if len(strngs) > 2:
            strng = "("+mode.join(strngs[0:2])+")"
            for piece in strngs[2:]:
                strng = "("+mode.join([strng,piece])+")"
        else:
            strng = "("+mode.join(strngs)+")"
    else:
        strng = '("{}"[{}])'.format(strng,field)
    log.debug('formatted query: '+ strng.lower())
    return strng.lower()


def list_tags(*tags):
    """generate list of all tags to be used in add_tag method, then rm dupes"""
    end_tags = []
    [end_tags.append(t) for t in tags]
    return sorted(set(end_tags))


def run_tests():
    log = log_it('Testing functions')
    tests = 0
    failures = 0

    from tempfile import mkstemp

    # test csv read/write/headers:
    tests += 1
    (csvfh1, csv_file1) = mkstemp('test','test',text=True)
    field_names1 = ['one','two','three']
    # print('field_names1: '+str(field_names1))
    csv_values1 = [{'one':'a', 'two':'b', 'three':'c'},
                   {'one':'d', 'two':'e', 'three':'f'}]
    write_out_csv(csv_file1,field_names1) #write headers
    write_out_csv(csv_file1,field_names1,csv_values1)
    field_names_read1 = get_field_header(csv_file1)
    # print('field_names_read1: '+str(field_names_read1))

    (csvfh2, csv_file2) = mkstemp('test2','test',text=True)
    values2 = [['g', 'h', 'i'], ['j', 'k', 'l']]
    csv_values2 = values_to_node_dict(values2,field_names_read1)
    write_out_csv(csv_file2,field_names1) #write headers
    write_out_csv(csv_file2,field_names_read1,csv_values2)
    field_names_read2 = get_field_header(csv_file2)
    # print('field_names_read2: '+str(field_names_read2))

    failures += 1 if field_names1 != field_names_read2 else 0
    log.warning('Test: fieldnames equal: %s', 'Y' if failures==0 else 'N')

    log.warn('Tests run: %s', tests)
    log.warn('Test failures: %s', failures)


if __name__ == '__main__':
    run_tests()
    pass
