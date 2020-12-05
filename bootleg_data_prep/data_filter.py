'''
This file
1. Reads in data from <data_dir>/orig. Assumes format is doc jsonl level.
2. Filters it based on the sentence_filter_func in utils.my_filter_funcs.
3. Filters entities based on if they exist in the data ans if they are one of the aliases candidates after max_candidates. You can turn off these filterings
by the no_filter_entities_data and no_filter_entities_cand, respecitively
4. Filters the data a final time based on train_in_candidates and the final filtered set of entities
5. Data is saved in <data_dir>/<subfolder_name>; A statistics file for slice generation is saved in <data_dir>/<subfolder_name>/stats

After this, run merge_shuff_split.py

Example run command:
python3.6 -m contextual_embeddings.bootleg_data_prep.data_filter --max_candidates 30 --processes 20 --train_in_candidates --sentence_filter_func sentence_filterUsaAlias --subfolder_name USA

FOR AIDA
python3.6 -m contextual_embeddings.bootleg_data_prep.data_filter --max_candidates 30 --processes 20 --train_in_candidates --sentence_filter_func sentence_filterQID --subfolder_name aidawiki --qid_filter_file data/aida/aida_qids.json --data_dir data/aida
'''
import argparse
import collections
import ujson as json
import multiprocessing
import os
import shutil
import time

from tqdm import tqdm

import bootleg_data_prep.utils.utils as utils
import bootleg_data_prep.utils.data_prep_utils as prep_utils
# DO NOT REMOVE THIS IMPORT STATEMENT
import bootleg_data_prep.utils.my_filter_funcs as my_filter_funcs
# DO NOT REMOVE THIS NEXT LINE
from bootleg_data_prep.utils.classes.entity_symbols import EntitySymbols

FILTER_FILE = "my_filter_funcs"

def parse_args():
    parser = argparse.ArgumentParser()

    # Input/output data directories
    parser.add_argument('--data_dir', type=str, default='data/wiki_dump', help='Directory for data to be saved.')
    parser.add_argument('--orig_dir', type=str, default='orig', help='Subdirectory to read data from.')
    parser.add_argument('--subfolder_name', type=str, default="filtered_data", help='Output subfolder name')

    # Sentence filtering utils 
    parser.add_argument('--sentence_filter_func', type=str, default='false_filter',
                        help='Name of sentence filter function in func filter file to call. Must return True (remove item) or False (keep item)')
    parser.add_argument('--prep_func', type=str, default="prep_standard", help="This will be called to load up anything for slicing that takes a while and you want to be done before hand")
    parser.add_argument('--prep_file', type=str, default="", help="This will be accessible to the prep_func")
    parser.add_argument('--filter_file', type=str, default="",
                        help = "path to JSON file with QIDs or ALIASES. If you use the sentence filter function sentence_filterQID or sentence_filterAliases then we'll load this files.")
    parser.add_argument('--disambig_file', type=str, default='/lfs/raiders7/0/lorr1/data/utils/disambig_qids.json')
    parser.add_argument('--max_candidates', type=int, default=30, help="Maximum number of entities to read in for each alias. When there are two many, some of the distance based slices get too small because of the large variability.")
    parser.add_argument('--no_filter_entities_data', action="store_true", help="if turned on, will not filter entity symbols after data filtering")
    parser.add_argument('--train_in_candidates', action='store_true', help='Make all training examples have true labels that are in the candidate set. THIS WILL ALMOST ALWAYS BE SET.')

    # After filtering candidate list by the maximum number of candidates (max_candidates), there may be some dangling entities in qid2title that are not longer ever referenced to.
    # By default, we then remove those from the list of entities as they are never seen during training. Turn this flag on to prevent that from happening.
    # The main use case is if we need to keep certain entities in the list for a downstream task, i.e. AIDA benchmark.
    parser.add_argument('--no_filter_entities_cand', action='store_true', help='Do not filter entities to be only ones used after max cands')
    parser.add_argument('--no_filter_disambig_entities', action='store_true', help='Do not filter entities that point to disambiguation pages')
    parser.add_argument('--benchmark_qids', default = "data/utils/benchmark_qids.json", type =str, help = "List of QIDS that should be kept in entity dump. This is to ensure the trained model has entity embeddings for these.")
    
    # Multiprocessing utilities 
    parser.add_argument('--processes', type=int, default=20)
    
    args = parser.parse_args()
    return args

def is_disambig_qid(qid, entity_symbols):
    title = entity_symbols.get_title(qid)
    if "(disambiguation)" in title.lower() or "(disambig)" in title.lower():
        return True
    return False

def init_process(args):
    extras_f, disambig_f = args
    global extras_global
    global disambig_qids_global
    extras_global = utils.load_pickle_file(extras_f)
    if disambig_f != "":
        disambig_qids_global = set(utils.load_json_file(disambig_f))
    else:
        disambig_qids_global = set()

# Filters data by the sentence filter function
def launch_subprocess_step1(args, out_dir, in_files):
    extras = {}
    if args.prep_func != "":
        extras = eval("{:s}.{:s}(args)".format(FILTER_FILE, args.prep_func))
    extras_f = os.path.join(out_dir, "extras.pkl")
    utils.dump_pickle_file(extras_f, extras)

    diambig_f = args.disambig_file

    all_process_args = []
    for i in range(len(in_files)):
        all_process_args.append(tuple([i+1, len(in_files), args, out_dir, in_files[i]]))
    print(f"Starting pool...")
    pool = multiprocessing.Pool(processes=args.processes, initializer=init_process, initargs=(tuple([extras_f, diambig_f]),))
    list_of_all_aliases = []
    for res in pool.imap(subprocess_step1, all_process_args, chunksize=1):
        list_of_all_aliases.append(res)
    print(f"Got all qids...closing pool")
    pool.close()
    return list_of_all_aliases


def subprocess_step1(all_args):
    i, total, args, out_dir, in_filepath = all_args
    start = time.time()
    out_fname = prep_utils.get_outfname(in_filepath)
    out_file = open(os.path.join(out_dir, out_fname), "w")
    print(f"Starting {i}/{total}. Reading in {in_filepath}. Outputting to {out_fname}. Outdir {out_dir}")
    # track the local frequency of alias-to-qids
    stats = {"filtered_func":0}
    all_aliases = set()
    # load file -- should be JSONL with each document as a distinct line
    with open(in_filepath, 'r') as in_file:
        for line in in_file:
            doc = json.loads(line.strip())
            title = doc['title']
            qid = doc["qid"]
            for sentence in doc['sentences']:
                aliases = sentence['aliases']
                qids = sentence['qids']
                text = sentence['sentence']
                spans = sentence['spans']
                sources = sentence.get('sources', ["gold"]*len(aliases))
                if len(spans) > 0 and type(spans[0]) is str:
                    spans = [list(map(int, s.split(":"))) for s in spans]
                if 'gold' in sentence:
                    gold = sentence['gold']
                elif 'anchor' in sentence:
                    gold = sentence['anchor']
                else:
                    gold = [True for _ in range(len(aliases))]
                if len(spans) > 0:
                    # Filter out sentences with invalid spans
                    if int(spans[-1][0]) >= len(text.split()):
                        continue 
                # qids_to_keep = extras_global['to_keep']
                if eval("{:s}.{:s}(args, aliases, qids, text, extras_global)".format(FILTER_FILE, args.sentence_filter_func)):
                    stats["filtered_func"] += 1
                    continue
                if not args.no_filter_disambig_entities:
                    filtered_list = list(filter(lambda x: (x[1] not in disambig_qids_global), zip(aliases, qids, spans, gold, sources)))
                    if len(filtered_list) <= 0:
                        continue
                    aliases_final, qids_final, spans_final, gold_final, sources_final = zip(*filtered_list)
                else:
                    aliases_final = aliases
                    qids_final = qids
                    spans_final = spans
                    gold_final = gold
                    sources_final = sources
                all_aliases.update(aliases_final)
                new_sent = {
                    'parent_qid': qid,
                    'parent_title': title,
                    'doc_sent_idx': sentence['doc_sent_idx'],
                    'sentence': text,
                    'aliases': aliases_final,
                    'qids': qids_final,
                    'spans': spans_final,
                    'gold': gold_final,
                    'sources': sources_final
                }
                out_file.write(json.dumps(new_sent) + '\n')
    out_file.close()
    print(f"Finished {i}/{total}. {len(all_aliases)} number aliases. Written to {out_fname}. {time.time() - start} seconds.")
    return all_aliases

# Filter entities by qids in data and by max candidates
def filter_entity_symbols(args, list_of_all_aliases, benchmark_qids, entity_symbols):
    all_aliases = set()
    for alias_set in tqdm(list_of_all_aliases):
        all_aliases = all_aliases.union(alias_set)
    stats = {"alias_not_in_all_aliases": 0, "qids_not_in_all_aliases": 0, "raw_qids": 0, "raw_aliases": 0}
    # Filter aliases by all aliases
    # Reset all qids because we may lose some more filtering by max candidates
    alias2qids = {}
    all_qids_local = set()
    for alias in entity_symbols.get_all_aliases():
        stats["raw_aliases"] += 1
        if alias not in all_aliases:
            stats["alias_not_in_all_aliases"] += 1
        num_used = 0
        for qid, count in sorted(entity_symbols.get_qid_count_cands(alias), key=lambda x: x[1], reverse=True):
            if num_used >= args.max_candidates:
                break
            if alias not in alias2qids:
                alias2qids[alias] = []
            alias2qids[alias].append([qid, count])
            all_qids_local.add(qid)
            num_used += 1
    qid2title = {}
    stats["raw_qids"] = len(entity_symbols.get_all_qids())
    stats["qids_not_in_all_aliases"] = len(all_qids_local)
    all_qids_local.update(benchmark_qids)
    for qid in all_qids_local:
        qid2title[qid] = entity_symbols.get_title(qid)
    # Get max candidates and max alias len
    max_candidates = 0
    max_alias_len = 0
    for alias in alias2qids:
        max_candidates = max(max_candidates, len(alias2qids[alias]))
        max_alias_len = max(max_alias_len, len(alias.split(" ")))
        alias2qids[alias] = sorted(alias2qids[alias], key=lambda x: x[1], reverse=True)
    print(json.dumps(stats, indent=4))
    return qid2title, alias2qids, max_candidates, max_alias_len

# Filter data by qids remaining in entity_symbols (some may be dropped because of max candidates)
# Also gather statistics of alias-qid counts for slice generation
def launch_subprocess_step2(args, out_dir, out_dir_stats, entity_symbols, files):

    temp_folder = prep_utils.get_outdir(out_dir, "temp_dump")
    print(f"Saving entity_symbol files in {temp_folder}")

    qid2title_f = os.path.join(temp_folder, "qid2title.json")
    alias2qids_f = os.path.join(temp_folder, "alias2qids.json")
    utils.dump_json_file(qid2title_f, entity_symbols.get_qid2title())
    utils.dump_json_file(alias2qids_f, entity_symbols.get_alias2qids())

    all_process_args = [tuple([i+1,
                               len(files),
                               args,
                               files[i],
                               out_dir,
                               out_dir_stats,
                               qid2title_f,
                               alias2qids_f
                               ]) for i in range(len(files))]
    pool = multiprocessing.Pool(processes=args.processes)
    list_of_stats = pool.map(subprocess_step2, all_process_args, chunksize=1)
    stats = prep_utils.aggregate_list_of_dictionaries(list_of_stats)
    pool.close()
    print(f"Cleaning up {temp_folder}")
    shutil.rmtree(temp_folder)
    return stats


def subprocess_step2(all_args):
    i, total, args, in_filepath, out_dir, out_dir_stats, qid2title_f, alias2qids_f = all_args

    qid2title = utils.load_json_file(qid2title_f)
    alias2qids = utils.load_json_file(alias2qids_f)
    out_fname = prep_utils.get_outfname(in_filepath)
    out_file = open(os.path.join(out_dir, out_fname), "w")
    out_file_stats = os.path.join(out_dir_stats, out_fname)
    print(f"Starting {i}/{total}. Reading in {in_filepath}. Outputting to {out_fname}. Outdir {out_dir}")

    statistics = {'total_mentions': 0, 'total_preserved': 0, 'total_dropped': 0}
    # map of gold -> alias -> QID -> count
    # this is for storing statistics over the training data;
    # True is for only when golds are true (i.e. statistics over raw wikipedia data)
    # False is for all golds (i.e. statistics over wikipedia golds and added candidates from our augmentation)
    alias_qid = {True: collections.defaultdict(lambda: collections.defaultdict(int)), False: collections.defaultdict(lambda: collections.defaultdict(int))}
    # load file -- should be JSONL with
    with open(in_filepath, 'r') as in_file:
        for line in in_file:
            sent_obj = json.loads(line.strip())
            statistics['total_mentions'] += len(sent_obj['qids'])
            # # LAUREL
            # for al in sent_obj['aliases']:
            #     if al not in alias2qids:
            #         print("BAD SENTENCE OBJ")
            #         print(json.dumps(sent_obj, indent=4))
            # items = list(filter(lambda x:(not args.train_in_candidates) or ((x[0] in alias2qids) and (x[1] in [y[0] for y in alias2qids[x[0]]])),
            #                     zip(sent_obj['aliases'], sent_obj['qids'], sent_obj['spans'], sent_obj['gold'])))
            # # LAUREL
            items = list(filter(lambda x: (not args.train_in_candidates) or (x[1] in [y[0] for y in alias2qids[x[0]]]),
                                zip(sent_obj['aliases'], sent_obj['qids'], sent_obj['spans'], sent_obj['gold'], sent_obj['sources'])))
            temp_len = len(items)
            items = list(filter(lambda x: x[1] in qid2title, items))
            # there should be no difference between these
            assert temp_len - len(items) == 0
            if len(items) == 0:
                continue
            aliases, qids, spans, golds, sources = zip(*items)
            statistics['total_dropped'] += len(sent_obj['qids']) - len(qids)
            if len(aliases) > 0:
                new_sent_obj = sent_obj
                new_sent_obj['aliases'] = aliases
                new_sent_obj['qids'] = qids
                new_sent_obj['spans'] = spans
                new_sent_obj['gold'] = golds
                new_sent_obj['sources'] = sources
                out_file.write(json.dumps(new_sent_obj) + '\n')
                # Update stats
                for alias, qid, gold in zip(aliases, qids, golds):
                    statistics['total_preserved'] += 1
                    alias_qid[gold][alias][qid] += 1
    assert statistics['total_mentions'] == (statistics['total_dropped'] + statistics['total_preserved'])
    out_file.close()
    print(f"Finished {i}/{total}. Data written to {os.path.join(out_dir, out_fname)}. Stats written to {os.path.join(out_dir_stats, out_fname)}")
    # save to tmp file in the out directory
    utils.dump_json_file(out_file_stats, alias_qid)
    return statistics


def aggregate_statistics_step2(args, out_dir_stats):
    out_file_with = os.path.join(out_dir_stats, "alias_qid_alldata_withaugment.json")
    out_file_without = os.path.join(out_dir_stats, "alias_qid_alldata_withoutaugment.json")
    print(f"Aggregating counts. Reading files from {out_dir_stats}. Outputting to {out_file_with} and {out_file_without}")
    in_files = [os.path.join(out_dir_stats, fname) for fname in os.listdir(out_dir_stats)]

    alias_qid_with = collections.defaultdict(lambda: collections.defaultdict(int))
    alias_qid_without = collections.defaultdict(lambda: collections.defaultdict(int))
    for i, file in enumerate(in_files):
        print(f"Processing {file}")
        counts = json.load(open(file, "r"))
        for gold, a_qid in counts.items():
            for k, v in a_qid.items():
                for kk, vv, in v.items():
                    if gold is True:
                        alias_qid_without[k][kk] += vv
                    alias_qid_with[k][kk] += vv

        print(f"Processed {i+1}/{len(in_files)} files.")

    utils.dump_json_file(out_file_with, alias_qid_with)
    utils.dump_json_file(out_file_without, alias_qid_without)
    # Clean up temporary files
    print("Cleaning up temporary files...")
    for file in in_files:
        os.remove(file)
    return

def main():
    gl_start = time.time()
    multiprocessing.set_start_method("forkserver", force=True)
    args = parse_args()
    print(json.dumps(vars(args), indent=4))

    # Get load data subfolder
    orig_subfolder = args.orig_dir
    load_dir = os.path.join(args.data_dir, orig_subfolder)
    print(f"Loading data from {load_dir}...")
    # Check to make sure the temporary directory we'll make is empty
    out_dir_step1 = prep_utils.get_outdir(args.data_dir, "filter_temp", remove_old=True)
    out_dir_step2 = prep_utils.get_outdir(args.data_dir, args.subfolder_name, remove_old=True)
    out_dir_stats_step2 = prep_utils.get_outdir(out_dir_step2, "stats", remove_old=True)
    vars(args)['out_dir_step1'] = out_dir_step1
    vars(args)['out_dir_step2'] = out_dir_step2
    vars(args)['out_dir_stats_step2'] = out_dir_stats_step2
    prep_utils.save_config(args, f"{args.subfolder_name}_filter_data_config.json")

    # load entity symbols
    print("="*10)
    print("Loading entity symbols...")
    start = time.time()
    entity_symbols = EntitySymbols(load_dir=os.path.join(load_dir, "entity_db/entity_mappings"))
    print(f"Loaded entity symbols with {entity_symbols.num_entities} entities and {len(entity_symbols.get_all_aliases())} aliases. {time.time() - start} seconds.")

    print(f"Loading data from {load_dir}...")
    files = prep_utils.glob_files(f"{load_dir}/*.jsonl")
    print(f"Loaded {len(files)} files.")
    print(f"Launching subprocess with {args.processes} processes...")
    ############################
    # FILTER DATA BY FUNCTION
    ############################
    list_of_all_aliases = launch_subprocess_step1(args, out_dir_step1, files)
    print(f"Done with round one filtering")
    ############################
    # FILTER ENTITIES BY DATA AND MAX CANDIDATES
    ############################

    # load QIDS from benchmark 
    benchmark_qids = []
    if os.path.exists(args.benchmark_qids):
        with open(args.benchmark_qids) as in_file: 
            benchmark_qids = json.load(in_file)
    print(f"Loaded {len(benchmark_qids)} QIDS from {args.benchmark_qids}")
    print(f"Filtering entity dump again")
    qid2title, alias2qids, max_candidates, max_alias_len = filter_entity_symbols(args, list_of_all_aliases, set(benchmark_qids), entity_symbols)
    # make new one to reindex eids
    entity_symbols_new = EntitySymbols(
        max_candidates=max_candidates,
        max_alias_len=max_alias_len,
        alias2qids=alias2qids,
        qid2title=qid2title
    )
    entity_symbols_new.dump(os.path.join(out_dir_step2, "entity_db/entity_mappings"))

    # Get load data subfolder
    load_dir = out_dir_step1
    print(f"Loading data from {load_dir}...")
    files = prep_utils.glob_files(f"{load_dir}/*.jsonl")
    print(f"Loaded {len(files)} files.")
    print(f"Launching subprocess with {args.processes} processes...")


    ############################
    # FILTER DATA BY ENTITIES AND GATHER STATISTICS
    ############################
    stats = launch_subprocess_step2(args, out_dir_step2, out_dir_stats_step2, entity_symbols_new, files)
    # Aggregate counts from temporary file
    aggregate_statistics_step2(args, out_dir_stats_step2)

    print("="*5, "Recall Statistics", "="*5)
    print(json.dumps(stats, indent=4))
    utils.dump_json_file(os.path.join(out_dir_stats_step2, "recall_stats.json"), stats)

    # Clean up
    shutil.rmtree(out_dir_step1)
    print(f"Finished data_filter in {time.time() - gl_start} seconds.")

if __name__ == '__main__':
    main()