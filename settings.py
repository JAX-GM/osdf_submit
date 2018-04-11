""" Settings, Variables
    for Stanford/JAXGM iHMP OSDF submissions
"""
import os
import yaml
import time


def load_string_from_file(filename):
    with open(os.path.join(os.curdir, filename)) as f:
        return f.read().strip()

class auth():
    dcc_user = load_string_from_file('auth/username.txt')
    dcc_pass = load_string_from_file('auth/password.txt')

class consented_users():
    try:
        list_of_consented_participants = load_string_from_file('auth/CONSENTED_MAP.sec')
    except Exception, e:
        list_of_consented_participants = ""


class data_node_ids():
        project = ''
        study = ''
        substudy = ''
        subject = []
        visit = []
        sample = []

class node_id_tracking:
    path = './data_files/prediabetes_node_id_tracking.csv'
    id_fields = ['node_type', 'internal_id', 'osdf_node_id',
                 'parent_node_type', 'parent_id', 'parent_node_id',
                 'date_submitted']

# data file names
NodeDataFiles = {
    # 'Project':      './data_files/project_info.yaml',
    # 'Study':        './data_files/study_info.yaml',
    # 'Subject':      './data_files/20160608-HMP2_metadata-subjects.csv',
    # 'Visit':        './data_files/20160609-HMP2_metadata-visits_jaxgm.csv',
    # 'Sample':       './data_files/20160610-HMP2_metadata-samples-final.csv',
    # 'Visit':        './data_files/20160623-HMP2_metadata-visits-ZOZOW1T.csv',
    # 'Sample':       './data_files/20160623-HMP2_metadata-samples-ZOZOW1T.csv',
    # 'Visit':        './data_files/20160708_HMP2_metadata-visits_jaxgm.csv',
    #
    # 'Sample':       './data_files/20160711_HMP2_metadata-r16sSamples_13J_seqd.csv',
    # 'r16sDnaPrep':  './data_files/20160708_HMP2_metadata-r16sDnaPrep_1st6.csv',
    # 'r16sRawSeqs':  './data_files/20160708_HMP2_metadata-r16sRaw_1st6.csv',
    # 'r16sTrimSeqs': './data_files/20160708_HMP2_metadata-r16sTrim_1st6.csv',
    #
    # 'Sample':       './data_files/HMP2_metadata-MasterSampleSheet_0801_new_jaxids-visits_20160804.csv',
    #
    # 'WgsDnaPrep':   './data_files/dnaPrep_mwgs_0801_20160808_merged_ZOZOW1T.csv',
    # 'WgsRawSeqs':   './data_files/20160824_mwgs_raw_ready.csv',
    #
    ## re-submissions/modifications:
    # 'Subject':      './data_files/20160926-subjects-consented.csv',
    # 'Visit':        './data_files/20160921-visits_jaxgm_newConsents.csv',
    # 'Sample':       './data_files/20160921-samples-lot16.csv',
    # 'Visit':        './data_files/20160927-HMP2-visits_ALL.csv',
    # 'Sample':       './data_files/20160927-HMP2-samples_ALL.csv',
    #
    # 'r16sDnaPrep':  './data_files/20160913-dnaPrep_16S_merged.csv',
    # 'r16sRawSeqs':  './data_files/20160920-16S-rawseqs.csv',
    # 'r16sTrimSeqs': './data_files/20160920-16S-trimseqs.csv',
    #
    # 'RnaPrep':      './data_files/20160913-metadata-dnaPrep_rna.csv',
    # 'MicrobRnaRaw': './data_files/20160920_rnaseq_raw.csv',
    #
    # 'metabolome':   './data_files/metabolome.csv',
    # 'metabolomeSample': './data_files/metabolome.csv',
    # 'hostAssayPrep':   './data_files/metabolome.csv',
    # 'hostTranscriptomicSample':'./data_files/brian_host_transcriptomics_raw_seq_set.csv',
    # 'hostTranscriptomicsRawSeqSet':   './data_files/host_transcriptomics_hostSeqSet.csv',
    # 'hostSeqPrep' : './data_files/host_seq_prep.csv',
    # 'proteome' :    './data_files/proteome.csv',
    # 'hostWgsSample':  './data_files/wgs_samples.csv',
    'hostRawSeqPrep' : './data_files/host_seq_prep.csv',
    # 'hostSeqPrepTranscriptome' : './data_files/host_transcriptomics_hostSeqSet.csv',
    #
    #
    #'r16sDnaPrep':  './data_files/20160930-dnaPrep_16S_all_merged.csv',
    #'r16sDnaPrep':  './data_files/20161122-16S_dnaPreps-to_submit_dupe_samples_ready.csv',
    #'r16sRawSeqs':  './data_files/20161121-16S_raw_seqfiles.csv',
    #'r16sTrimSeqs': './data_files/20161121-16S_trim_seqfiles.csv',
    #'r16sRawSeqs':  './data_files/20161207-16S-preps_missing_files-raw.csv',
    #'r16sTrimSeqs':  './data_files/20161207-16S-preps_missing_files-clean.csv',

    #'WgsDnaPrep':   './data_files/20161212-mwgs-samples_merged_checksummed_matches.csv',
    #'WgsRawSeqs':   './data_files/20161212-mwgs-samples_merged_checksummed_matches_consented_post-net-error1.csv',

    #'RnaPrep':      './data_files/20161219-samples_rnaseq_merged.csv',
    #'RnaPrep':      './data_files/20161219-samples_rnaseq_merged_2A.csv',
    #'MicrobRnaRaw': './data_files/20161219-samples_rnaseq_merged_checksummed_part2.csv'
    # 'RnaPrep':      './data_files/20170206_rnaseq_prep_ids_raws.csv',
    # 'MicrobRnaRaw': './data_files/20170206_rnaseq_prep_ids_raws.csv',

    # 'r16sDnaPrep':  './data_files/20170202-samples_16S_merged.csv',
    # 'r16sRawSeqs':  './data_files/20170202-samples_16S_rawseqs_2.csv',
    # 'r16sTrimSeqs': './data_files/20170210-samples_16S_trimseqs.csv',

    # 'WgsDnaPrep':   './data_files/20170206-samples_mWGS_merged.csv',
    # 'WgsRawSeqs':   './data_files/20170206-samples_mWGS_merged_merged_parent_ids.csv',

    # Non-consenting!
    # 'Visit':        './data_files/HMP2_metadata-visitsNoConsent.csv',
    # 'Sample':       './data_files/HMP2_metadata-SamplesNoConsent.csv',
    # 'Visit':        './data_files/HMP2_metadata-visits_NotSubmitted-20170212.csv',
    # 'Sample':       './data_files/HMP2_metadata-samples_NotSubmitted-20170212.csv',

    # 'Subject':      './data_files/HMP2_metadata-subjects_NotSubmitted-20170212.csv',

    #'Visit':        './data_files/Wenyu_Visits.csv',
    #'Sample':       './data_files/SaraProteome.csv',
    #'hostAssayPrep':   './data_files/SaraProteome.csv',
    #'proteome':   './data_files/SaraProteome.csv',

    #'Sample':       './data_files/KevinMetabolome2017.csv',
    #'hostSeqPrep':   './data_files/KevinMetabolome2017.csv',
    #'hostAssayPrep':   './data_files/KevinMetabolome2017.csv',
    #'metabolome':   './data_files/KevinMetabolome2017.csv',

    #'Visit':        './data_files/RezaFiles.csv',
    #'hostSeqPrep':   './data_files/RezaFiles.csv',
    #'hostTranscriptomicsRawSeqSet':   './data_files/RezaFiles.csv',


    # ...plus all dnaPreps, rawFiles, etc....
    # 'RnaPrep':      './data_files/20170213-grep-rnaseq_missing_sample_preps.csv',
    # 'RnaPrep':      './data_files/20170213-grep-rnaseq_missing_sample_preps_2.csv',
    # 'RnaPrep':      './data_files/20170213-grep-rnaseq_missing_sample_preps_3.csv',
    # 'RnaPrep':      './data_files/20170213-grep-rnaseq_missing_sample_preps_5_redo.csv',
    # 'WgsDnaPrep':   './data_files/20170214_mwgs_prep_ids-unsubmitted.csv',
    # 'WgsDnaPrep':   './data_files/20170214_mwgs_prep_ids-unsubmitted_1.csv',
    # 'r16sDnaPrep':  './data_files/20170216_16S_dna_prep.csv',

    # ... and then the raw files...
    # 'r16sRawSeqs':  './data_files/20170202-samples_16S_rawseqs_2.csv',
    # 'WgsRawSeqs':   './data_files/20170223_mwgs_raw_1.csv',
    # 'WgsRawSeqs':   './data_files/20170223_mwgs_raw_2.csv',
    # 'WgsRawSeqs':   './data_files/20170223_mwgs_raw_3.csv',
    # 'WgsRawSeqs':   './data_files/20170223_mwgs_raw_4.csv',
    # 'WgsRawSeqs':   './data_files/20170223_mwgs_raw_5.csv',
    # 'MicrobRnaRaw': './data_files/20170223_rna_raw_2.csv',
    # 'MicrobRnaRaw': './data_files/20170223_rna_raw_2.csv_invalid_records.csv',

    # 'r16sRawSeqs':  './data_files/20170301_16S_raw.csv',
    # 'r16sRawSeqs':  './data_files/20170301_16S_raw_NO.csv',
    # 'r16sTrimSeqs': './data_files/20170301_16S_clean_1.csv',
    # 'r16sTrimSeqs': './data_files/20170301_16S_clean_NO.csv',
    # 'r16sRawSeqs':  './data_files/20170306_submittable_16S_nodes_raw_clean.csv',
    # 'r16sTrimSeqs': './data_files/20170306_submittable_16S_nodes_clean.csv',
    # 'r16sDnaPrep':  './data_files/20170308_submittable_16S_nodes_preps_raw_clean.csv',
    # 'r16sRawSeqs':  './data_files/20170308_submittable_16S_nodes_preps_raw_clean.csv',
    # 'r16sTrimSeqs': './data_files/20170308_submittable_16S_nodes_clean.csv',

    # 'WgsDnaPrep':   './data_files/20170427_mwgs_preps.csv',
    # 'WgsDnaPrep':   './data_files/20170515_mwgs_prep_raw.csv',
    # 'WgsRawSeqs':   './data_files/20170515_mwgs_prep_raw.csv',

    # 'Sample':       './data_files/20170515-samples_mwgs_missing.csv',

    # Last Lots
    # 'Visit':        './data_files/20170629_visits_missing.csv',
    #'Sample':       './data_files/20170630_sample_missing.csv',
    #'r16sDnaPrep':  './data_files/20170630_sample_missing.csv',
    #'r16sRawSeqs':  './data_files/20170630_sample_missing.csv',
    #'r16sTrimSeqs': './data_files/20170630_16Scleans_missing.csv',

    }

'''CURDATE = time.strftime("%Y%m%d")
NodeRetrievalFiles = {
    'subject':      './osdf_node_records/{}_subjects.csv'.format(CURDATE),
    'visit':        './osdf_node_records/{}_visits.csv'.format(CURDATE),
    'sample':       './osdf_node_records/{}_samples.csv'.format(CURDATE),
    '16s_dna_prep':        './osdf_node_records/{}_r16sdnapreps.csv'.format(CURDATE),
    '16s_raw_seq_set':     './osdf_node_records/{}_r16srawseqs.csv'.format(CURDATE),
    '16s_trimmed_seq_set': './osdf_node_records/{}_r16strimseqs.csv'.format(CURDATE),
    'wgs_dna_prep':        './osdf_node_records/{}_wgsdnapreps.csv'.format(CURDATE),
    'wgs_raw_seq_set':     './osdf_node_records/{}_wgsrawseqs.csv'.format(CURDATE),
    'rnaseq_prep':         './osdf_node_records/{}_rnapreps.csv'.format(CURDATE),
    'microb_transcriptomics_raw_seq_set': './osdf_node_records/{}_microbrnarawseqs.csv'.format(CURDATE),
    }

NodeUpdateFiles = {
    'Subject':      './osdf_node_records/20170220_update_subjects.csv',
    'Visit':        './osdf_node_records/20170220_update_visits.csv',
    'Sample':       './osdf_node_records/20170505_samples_correct_N-ST.csv',

    'WgsDnaPrep':   './osdf_node_records/20170220_update_wgsdnapreps.csv',
    'WgsRawSeqs':   './osdf_node_records/20170324_update_wgsrawseqs.csv',
    'RnaPrep':      './osdf_node_records/20170220_update_rnapreps.csv',
    'MicrobRnaRaw': './osdf_node_records/20170210_update_microbrnarawseqs.csv',

    'r16sDnaPrep':  './osdf_node_records/20170308_update_r16sPreps.csv',
    'r16sDnaPrep':  './osdf_node_records/20170302_update_r16sdnapreps_2.csv',
    'r16sRawSeqs':  './osdf_node_records/20170302_update_r16srawseqs.csv',
    'r16sTrimSeqs': './osdf_node_records/20170307_update_r16sTrimSeqs_comments.csv',
    'r16sDnaPrep':  './osdf_node_records/20170317_update_16SdnaPreps_linkage.csv',
    }
'''

class node_hierarchy:
  """node_hierarchy lists nodes in hierarchical tree
     => project.study.subject.visit.sample.[16S,wgs,rnaseq,host,genome,...]
        16S[dnaprep,raw,trim]
        wgs[dnaprep,raw,assembled]
        rnaseq[dnaprep,raw,...]
        host[dnaprep,raw,...]
  """
  node_tree = [
    'project', [
      'study', [
        'subject', [
          'visit', [
            'sample',[
              [ 'r16sDnaPrep',[
                    'r16sRawSeqSet',[
                       'r16sTrimSeqSet',] ] ],
              [ 'wgsDnaPrep', [
                    'wgsRawSeqSet',[
                      'wgsAssembledSeqSet', ],
                    'MicrobTranscriptomicsRawSeqSet'
                    ] ],
              ['hostAssay', 'etc',],
              ['metabolome', 'etc',],
              ['hostTranscriptomicsRawSeqSet', 'etc',],
              ['hostSeqPrep','etc',],
              ['proteome','etc',],
            ]
          ]
        ]
      ]
    ]
  ]


if __name__ == '__main__':
    from pprint import pprint
    print('nodes')
    n = node_hierarchy()
    pprint(n.node_tree)
