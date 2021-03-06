import pandas as pd
import numpy as np
import json
import traceback

pair_path      = 'esmall_pairs.json'
bugrepo_path   = 'esmall_clear.json'

pair_topath    = 'esmall_pairs.csv'
bugrepo_topath = 'esmall_bug_repos.csv'
summary_topath = 'esmall_summary.csv'

def get_pair_frame(file_path):
    dicList=[json.loads(line) for line in open(file_path)]
    res_list = []
    for dic in dicList:
        res_list.append([dic['bug1'], dic['bug2'], dic['dec']])
    res_list = pd.DataFrame(res_list, columns=['bug1', 'bug2', 'dec'])
    return res_list

def extract_bugrepo(file_path):
    '''
        Json like this
        {
            "_id"         :{"$oid":"52e9a43354dc1c24f597bef8"},
            "bug_id"      :"214065",
            "product"     :"BIRT",
            "description" :"Description:\n[Regression] ...",
            "bug_severity":"normal",
            "dup_id"      :[],
            "short_desc"  :"[Regression]Group TOC are create ... PDF",
            "priority"    :"P3",
            "version"     :"2.3.0",
            "component"   :"Report Engine",
            "delta_ts"    :"2008-01-02 21:38:46 -0500",
            "bug_status"  :"CLOSED",
            "creation_ts" :"2008-01-02 00:34:00 -0500",
            "resolution"  :"FIXED"
        }
    '''
    dicList=[json.loads(line) for line in open(file_path)]
    res_list = []
    for dic in dicList:
        bug_id       = dic['bug_id'] if 'bug_id' in dic else ''
        product      = dic['product'] if 'product' in dic else ''
        description  = dic['description'] if 'description' in dic else ''
        bug_severity = dic['bug_severity'] if 'bug_severity' in dic else ''
        dup_id       = dic['dup_id'] if 'dup_id' in dic else ''
        short_desc   = dic['short_desc'] if 'short_desc' in dic else ''
        priority     = dic['priority'] if 'priority' in dic else ''
        version      = dic['version'] if 'version' in dic else ''
        component    = dic['component'] if 'component' in dic else ''
        delta_ts     = dic['delta_ts'] if 'delta_ts' in dic else ''
        bug_status   = dic['bug_status'] if 'bug_status' in dic else ''
        creation_ts  = dic['creation_ts'] if 'creation_ts' in dic else ''
        resolution   = dic['resolution'] if 'resolution' in dic else ''
        
        res_list.append([bug_id, product, description, bug_severity, dup_id,\
                        short_desc, priority, version, component, delta_ts,\
                        bug_status, creation_ts, resolution
                        ])
    res_list = pd.DataFrame(res_list, columns=['bug_id', 'product', 'description', 'bug_severity',
                                               'dup_id', 'summary', # change short_desc to summary
                                               'priority', 'version', 'component', 'delta_ts', 'bug_status',
                                               'creation_ts', 'resolution'
                                              ])
    return res_list

def gen(col, bug_repos, pairs):
    '''
        chose specific column
    '''
    res = []
    for i, r in pairs.iterrows():
        try:
            res.append([
                    bug_repos[bug_repos['bug_id'] == str(r['bug1'])][col].values[0],
                    bug_repos[bug_repos['bug_id'] == str(r['bug2'])][col].values[0],
                    r['dec']
                ])
        except:
            print(traceback.print_exc())
    res = pd.DataFrame(res, columns=[col + '_bug1', col + '_bug2', 'dec'])
    return res

if __name__ == '__main__':
    pairs = get_pair_frame(pair_path)
    bug_repos = extract_bugrepo(bugrepo_path)
    summary = gen('summary', bug_repos, pairs)
    pairs.to_csv(pair_topath, index=False, encoding='GB18030')
    bug_repos.to_csv(bugrepo_topath, index=False, encoding='GB18030')
    summary.to_csv(summary_topath, index=False, encoding='GB18030')

    
    