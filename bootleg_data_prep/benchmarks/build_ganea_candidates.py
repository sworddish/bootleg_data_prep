''' 
This file: 
1. Processes candidate list generated by Ganea and Hoffman 
2. Maps to QIDS
3. Writes to file 

''' 

import ujson as json
import sys
import os
sys.path.append(os.path.join(sys.path[0], "../"))
import argparse
from language import ENSURE_ASCII
import bootleg_data_prep.utils.data_prep_utils as prep_utils


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_dir', type=str, default='contextual_embeddings/bootleg_data_prep/benchmarks/ganea/', help='Path to raw candidate files')
    parser.add_argument('--out_dir', type = str, default = 'processed', help = 'output directory for processed ganea candidates')
    parser.add_argument('--title_to_qid', type=str, default='/dfs/scratch1/nguha/disambiguation/frozen_data/utils/title_to_all_ids.jsonl')
    parser.add_argument('--redirect_map', type=str, default='contextual_embeddings/bootleg_data_prep/benchmarks/aida/raw_aida/redirects_map.txt', help='Path to file mapping titles to WPID')
    parser.add_argument('--topK', default = 30, help = 'Number of candidates to include')
    args = parser.parse_args()
    return args

def get_title_from_url(url):
    return url.replace("http://en.wikipedia.org/wiki/", "").replace("_", " ")

def get_title_to_wpid(args):
    oldtitle2wpid = {}
    with open(args.redirect_map) as in_file: 
        # line looks like this:
        # http://en.wikipedia.org/wiki/Mike_Conley,_Sr.	Mike Conley Sr.
        for line in in_file: 
            old_title, wpid = line.strip().split("\t")
            oldtitle2wpid[old_title] = wpid
    print(f"Loaded {len(oldtitle2wpid)} title redirect pairs from {args.redirect_map}")
    return oldtitle2wpid




class QIDMapper:

    def __init__(self, wpid2qid, title2qid, redirect_title2wpid):
        
        self.wpid2qid = wpid2qid # maps wpid to QID
        self.title2qid = title2qid # maps wikipedia page title to QID 
        self.redirect_title2wpid = redirect_title2wpid # maps [old] wikipedia page title to WPID (frequently result of redirect)



    def get_qid(self, title, wpid): 

        # Get QID based on title 
        if title in self.title2qid:
            return self.title2qid[title]
        
        # Get redirected title 
        if title in self.redirect_title2wpid: 
            wpid = self.redirect_title2wpid[title]
            if wpid in self.wpid2qid:
                return self.wpid2qid[title]

        # Get based on WPID
        if wpid in self.wpid2qid:
            return self.wpid2qid[wpid]
    
        return None 
        
 
def process_files(qm, args):

    files = prep_utils.glob_files(os.path.join(args.data_dir, 'raw/*'))
    dropped_wpids = set()
    alias_to_candidates = {}
    for file in files: 
        with open(file) as in_file: 
            for line in in_file: 
                items = line.strip().split("\t")
                alias = items[2].lower()
                assert items[5] == 'CANDIDATES', items
                assert items[len(items)-2] == 'GT:', items
                candidates = items[6:len(items)-2]
                filtered_candidates = []
                for candidate_tuple in candidates:
                    tup = candidate_tuple.split(",")
                    title = ','.join(tup[2:])
                    wpid = tup[0]
                    qid = qm.get_qid(title, wpid)
                    if qid is None:  
                        dropped_wpids.add(wpid)
                    else:
                        filtered_candidates.append([qid, float(tup[1])])
                
                alias_to_candidates[alias] = filtered_candidates
    
    for alias in alias_to_candidates:
        cands = alias_to_candidates[alias]
        alias_to_candidates[alias] = sorted(cands, key=lambda x: x[1], reverse=True)[:30]
    print(f"Loaded candidates for {len(alias_to_candidates)} aliases")
    return alias_to_candidates

def main():
    args = parse_args()
    print(json.dumps(vars(args), indent=4))

    redirect_title2wpid = get_title_to_wpid(args)
    title2qid, wpid2qid, _ = prep_utils.load_qid_title_map(args.title_to_qid, lower_case = False)
    qm = QIDMapper(wpid2qid, title2qid, redirect_title2wpid)

    cands = process_files(qm, args)
    out_dir = prep_utils.get_outdir(args.data_dir, args.out_dir)
    with open(os.path.join(out_dir, 'cands.json'), 'w', encoding='utf8') as out_file:
        json.dump(cands, out_file, ensure_ascii=ENSURE_ASCII)

if __name__ == '__main__':
    main()
