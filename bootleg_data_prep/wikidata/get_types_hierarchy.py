'''
This file 

1. Reads in all entity-entity relations
2. Saves all triples for properties: P31 (instance of),  P106 (occupation)

to run: 
python3.6 -m processor.get_types

''' 

    
import os, json, argparse, time
from glob import glob

import marisa_trie
from tqdm import tqdm 
from multiprocessing import set_start_method, Pool

from collections import defaultdict

import simple_wikidata_db.utils as utils

from bootleg_data_prep.language import ENSURE_ASCII

ENSURE_ASCII = False

# OCCUPATION = 'P106' is not needed since it's only for person 
INSTANCE_OF = 'P31'
SUBCLASS_OF = 'P279'
# Get instance of, subclass of, and occupation (for people)
TYPE_PIDS = {INSTANCE_OF, SUBCLASS_OF}

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type = str, default = 'wikidata', help = 'path to output directory')
    parser.add_argument('--out_dir', type = str, default = 'types_output', help = 'path to output directory')
    parser.add_argument('--processes', type = int, default = 80, help = "Number of concurrent processes to spin off. ")
    return parser 

def launch_entity_table(entity_files, qid_to_title, out_dir, args):
    print(f"Starting with {args.processes} processes")
    pool = Pool(processes = args.processes)
    messages = [(i, len(entity_files), entity_files[i], out_dir) for i in range(len(entity_files))]
    pool.map(load_entity_file, messages, chunksize=1)
    merge_and_save(out_dir, qid_to_title)
    return

def load_entity_file(message):
    start = time.time()
    job_index, num_jobs, filename, out_dir = message
    type_dict = defaultdict(set)
    for triple in utils.jsonl_generator(filename):
        qid, property_id, value = triple['qid'], triple['property_id'], triple['value']
        if property_id in TYPE_PIDS:
            type_dict[qid].add(value)

    out_f = open(os.path.join(out_dir, f"_out_{job_index}.json"), "w", encoding='utf8')
    print(f"Found {len(type_dict)}")
    type_dict = dict(type_dict)
    # convert to list type for json serialization
    for k in list(type_dict.keys()):
        type_dict[k] = list(type_dict[k])
    json.dump(type_dict, out_f, ensure_ascii=ENSURE_ASCII)
    print(f"Finished {job_index} / {num_jobs}...{filename}. Fetched types for {len(type_dict)} entities. {time.time() - start} seconds.")
    return dict(type_dict)

def merge_and_save(out_dir, qid_to_title):
    type_dict = defaultdict(set)
    type_freq = defaultdict(int)
    in_files = glob(os.path.join(out_dir, f"_out_*.json"))
    for f in tqdm(in_files):
        d = json.load(open(f, 'r', encoding="utf-8"))
        for qid, types in d.items():
            for qtype in types:
                type_dict[qid].add(qtype)
                type_freq[qtype] += 1
    # Sort types based on most to least frequent
    sorted_typs = sort_types(type_dict, type_freq)
    write_types(out_dir, sorted_typs, qid_to_title)

    with open(os.path.join(out_dir, 'type_freqs.json'), 'w', encoding='utf8') as out_file:
        json.dump(type_freq, out_file, ensure_ascii=ENSURE_ASCII)
    print(f"Removing the temporary files")
    for file in in_files:
        os.remove(file)

    return

# Sort types based on most to least frequent
def sort_types(type_dict, type_freq):
    sorted_types = {} # map QID to list of ordered type qids
    for qid, types in type_dict.items():
        stypes = sorted(types, key=lambda i: type_freq[i], reverse=True)
        sorted_types[qid] = stypes 
    return sorted_types

def write_types(out_dir, type_list, qid_to_title):
    typeqid2title = {}
    typeqid2qid = {}
    # Get all types
    for qid, types in tqdm(type_list.items(), desc="Iterating type list"): 
        for qtype in types:
            typeqid2title[qtype] = qid_to_title.get(qtype, qtype)
    # Build the type mapping for types only (not all QIDs)
    for qid in tqdm(typeqid2title.keys(), desc="Iterating type titles"):
        typeqid2qid[qid] = type_list.get(qid, [])
    with open(os.path.join(out_dir, 'wikidatatypeqid2typeqid.json'), 'w', encoding='utf8') as out_file:
        json.dump(typeqid2qid, out_file, ensure_ascii=ENSURE_ASCII)

    with open(os.path.join(out_dir, 'wikidatatypeqid2title.json'), 'w', encoding='utf8') as out_file:
        json.dump(typeqid2title, out_file, ensure_ascii=ENSURE_ASCII)
    print(f"Writtten to {out_dir}")

def read_in_wikidata_title(args):
    fdir = os.path.join(args.data, "processed_batches", "labels")
    wikidata_files = utils.get_batch_files(fdir)
    id_to_title = {}
    for file in tqdm(wikidata_files, desc="Reading in wikidata files"):
        with open(file, "r", encoding="utf-8") as in_f:
            for line in in_f:
                line = json.loads(line)
                id_to_title[line["qid"]] = line["label"]
    return id_to_title

def main():
    set_start_method('spawn')
    start = time.time()
    args = get_arg_parser().parse_args()

    out_dir = os.path.join(args.data, args.out_dir)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    print(f"Loading wikidata qids to titles")
    qid_to_title = read_in_wikidata_title(args)

    fdir = os.path.join(args.data, "processed_batches", "entity_rels")
    entity_table_files = utils.get_batch_files(fdir)
    launch_entity_table(entity_table_files, qid_to_title, out_dir, args)
    print(f"Finished in {time.time() - start}")

if __name__ == "__main__":
    main()