import pandas as pd
import numpy as np
import json
import traceback

pair_path = 'esmall_pairs.json'
bugrepo_path = 'esmall_clear.json'


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
        res_list.append([dic['bug_id'], dic['product'], dic['description'], dic['bug_severity'],\
                         dic['dup_id'], dic['short_desc'], dic['priority'], dic['version'],\
                         dic['component'], dic['delta_ts'], dic['bug_status'], dic['creation_ts'],\
                         dic['resolution']
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
    pairs.to_csv('esmall_pairs.csv', index=False, encoding='GB18030')
    bug_repos.to_csv('esmall_bug_repos.csv', index=False, encoding='GB18030')
    summary.to_csv('esmall_summary.csv', index=False, encoding='GB18030')

    
    