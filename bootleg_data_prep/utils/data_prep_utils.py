# A random assortment of utility functions that are helpful for data prep
import glob
import shutil

import marisa_trie
import psutil
from jsonlines import jsonlines
from tqdm import tqdm
import time
import ujson as json
from collections import defaultdict
from datetime import datetime
import os

from bootleg_data_prep.language import stem, pos_tag, bigrams, PUNC_TRANSLATION_TABLE, VERBS, EXTENDED_STOPWORDS, NOUNS, WORDS_TO_AVOID, get_lnrm
from bootleg_data_prep.utils import utils


def print_memory():
    process = psutil.Process(os.getpid())
    print(f"{int(process.memory_info().rss)/1024**3} GB ({process.memory_percent()} %) memory used process {process}")

def load_qid_title_map(title_to_qid_fpath):
    start = time.time()
    title_to_qid = {}
    qid_to_all_titles = defaultdict(set)
    wpid_to_qid = {}
    qid_to_title = {}
    all_rows = []
    with jsonlines.open(title_to_qid_fpath, 'r') as in_file:
        for items in tqdm(in_file, total=15162208):
            # the title is the url title that may be redirected to another wikipedia page
            qid, title, wikidata_title, wikipedia_title, wpid = items['qid'], items['title'], items['wikidata_title'], items['wikipedia_title'], items['id']
            if str(qid) == "-1":
                continue
            all_rows.append([qid, title, wikidata_title, wikipedia_title, wpid])
            qid_to_all_titles[qid].add(wikidata_title)
            qid_to_all_titles[qid].add(wikipedia_title)
            qid_to_all_titles[qid].add(title)

            # We want to keep the wikipedia titles
            if wikipedia_title in title_to_qid and qid != title_to_qid[wikipedia_title]:
                print(f"Wikipedia Title {wikipedia_title} for {title_to_qid[wikipedia_title]} already exists and we want {qid}")
            title_to_qid[wikipedia_title] = qid
            # if wikipedia_title.lower() in title_to_qid and qid != title_to_qid[wikipedia_title.lower()]:
            #     print(f"Wikipedia Title {wikipedia_title.lower()} for {title_to_qid[wikipedia_title.lower()]} already exists and we want {qid}")
            # title_to_qid[wikipedia_title.lower()] = qid
            qid_to_title[qid] = wikipedia_title
            wpid_to_qid[wpid] = qid

        # The title represents a redirect. We only want to add them if the redirect title does not already point to a QID from Wikipedia.
        for item in tqdm(all_rows, desc="Adding extra titles"):
            qid, title, wikidata_title, wikipedia_title, wpid = item
            if title not in title_to_qid:
                title_to_qid[title] = qid

    print(f"Loaded title-qid map for {len(title_to_qid)} titles from {title_to_qid_fpath}. {time.time() - start} seconds.")
    return title_to_qid, qid_to_all_titles, wpid_to_qid, qid_to_title

def load_wikidata_alias_to_qid(wd_alias_fdir):
    # Load dictionary mapping QIDs to aliases. This is generated from wikidata.
    # On Raiders, this is stored as a collection of jsonl files.
    print(f"Loading QID-to-alias map from {wd_alias_fdir}")
    start = time.time()
    files = glob.glob(f"{wd_alias_fdir}/*.jsonl")
    print(f"Collected {len(files)} files.")
    alias_to_qid = defaultdict(set)
    for file in tqdm(files):
        with open(file, 'r', encoding="utf-8") as in_file:
            for line in in_file:
                data = json.loads(line.strip())
                qid = data['qid']
                alias = data['alias']
                alias_to_qid[alias].add(qid)
    print(f"Finished! Collected QIDs for {len(alias_to_qid)} aliases. {time.time() - start} seconds.")
    return alias_to_qid


def aggregate_list_of_nested_dictionaries(list_of_nested_dicts):
    account = {}
    for dictionary in tqdm(list_of_nested_dicts):
        for k, dictionary2 in dictionary.items():
            if not k in account:
                account[k] = {}
            for kk, vv in dictionary2.items():
                if not kk in account[k]:
                    account[k][kk] = 0
                account[k][kk] += vv
    return account

def aggregate_list_of_dictionaries(list_of_dicts):
    account = {}
    for dictionary in tqdm(list_of_dicts):
        for k, v in dictionary.items():
            if not k in account:
                account[k] = 0
            account[k] += v
    return account

def normalize_count_nested_dict(nested_dict):
    res = {}
    for key1 in nested_dict:
        total_cnt = sum(nested_dict[key1].values())
        res[key1] = {key2: cnt/total_cnt for key2, cnt in nested_dict[key1].items()}
    return res

def glob_files(path):
    files = glob.glob(path)
    return list(filter(lambda x: not os.path.isdir(x), files))

def save_config(args, filename="config.json"):
    vars(args)["current_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(args, "out_subdir"):
        utils.dump_json_file(filename=os.path.join(args.data_dir, args.out_subdir, filename), contents=vars(args))
    else:
        utils.dump_json_file(filename=os.path.join(args.data_dir, filename), contents=vars(args))

def get_outfname(in_filepath, ending="jsonl"):
    # Gets basename and removes jsonl
    out_fname = os.path.splitext(os.path.basename(in_filepath))[0]
    hash_v = hash(in_filepath)
    # Removes old + hash() parts of file
    out_fname_base = out_fname.rsplit("_", maxsplit=1)[0]
    return f"{out_fname_base}_{hash_v}.{ending}"

def get_outdir(save_dir, subfolder, remove_old=False):
    out_dir = os.path.join(save_dir, subfolder)
    if remove_old and os.path.exists(out_dir):
        print(f"Deleting {out_dir}...")
        shutil.rmtree(out_dir)
    print(f"Making {out_dir}")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


# If we find any alias in aliases that is a strict supset of an alias in superset_aliases, we remove it
# Ex:
# aliases: ['victoria', 'beckham']
# spans: ['0:1', '3:4']
# superset_aliases: ['david beckham', 'london', 'london']
# superset_spans: ['2:4', '18:24', '31:37']
# Walk through spans and superset_spans
# If span_l < supserspan_l, not a subset, increment spans_idx
# If span_r > superspan_r, not a subset, increment superspans_idx
def filter_superset_aliases(aliases, spans, superset_aliases, superset_spans):
    if len(superset_aliases) == 0:
        return aliases, spans
    spans_l = [sp[0] for sp in spans]
    spans_r = [sp[1] for sp in spans]
    spans_super_l = [sp[0] for sp in superset_spans]
    spans_super_r = [sp[1] for sp in superset_spans]
    assert list(sorted(spans_l)) == spans_l
    assert list(sorted(spans_super_l)) == spans_super_l
    span_idx = 0
    super_span_idx = 0
    idxs_to_keep = []
    while span_idx < len(spans_l) and super_span_idx < len(spans_super_l):
        if spans_l[span_idx] < spans_super_l[super_span_idx]:
            idxs_to_keep.append(span_idx)
            span_idx += 1
        else: #spans_l[span_idx] >= spans_super_l[super_span_idx]
            if spans_r[span_idx] > spans_super_r[super_span_idx]:
                super_span_idx += 1
            else: # drop example
                span_idx += 1
    while span_idx < len(spans_l):
        idxs_to_keep.append(span_idx)
        span_idx += 1
    spans_to_keep = []
    aliases_to_keep = []
    for idx in idxs_to_keep:
        aliases_to_keep.append(aliases[idx])
        spans_to_keep.append([spans_l[idx], spans_r[idx]])
    return aliases_to_keep, spans_to_keep

# Given a sequence of [a, b, c, ...] with positions in sentence [0, 2, 4, ...]. Filter the sequence so that only items with postition pos
# such that if L = spans[0]-threshold, R = spans[1]+threshold, then L <= pos and pos < R
def filter_seq_by_span(seq, seq_pos, spans, threshold=5):
    assert len(spans) == 2
    filt_seq = []
    # spans[1] is exclusive
    for pos, s in zip(seq_pos, seq):
        if pos < spans[1]+threshold and spans[0]-threshold <= pos:
            filt_seq.append(s)
    return filt_seq

def filter_seq_by_span_right(seq, seq_pos, spans, threshold=5):
    assert len(spans) == 2
    filt_seq = []
    filt_seq_pos = []
    # spans[1] is exclusive
    for pos, s in zip(seq_pos, seq):
        if pos < spans[1]+threshold and pos >= spans[1]:
            filt_seq.append(s)
            filt_seq_pos.append(pos)
    return filt_seq, filt_seq_pos

def filter_seq_by_span_left(seq, seq_pos, spans, threshold=5):
    assert len(spans) == 2
    filt_seq = []
    filt_seq_pos = []
    # spans[1] is exclusive
    for pos, s in zip(seq_pos, seq):
        if spans[0]-threshold <= pos and pos < spans[0]:
            filt_seq.append(s)
            filt_seq_pos.append(pos)
    return filt_seq, filt_seq_pos

# Get sequence of items to the left or right of spans within a threshold window
def filter_seq_by_span_dir(seq, seq_pos, spans, threshold=5, left=False):
    if left:
        return filter_seq_by_span_left(seq, seq_pos, spans, threshold=threshold)
    return filter_seq_by_span_right(seq, seq_pos, spans, threshold=threshold)

# This gives 3 index windows around each comma position; used for the consistency slice
def extract_allowed_comma_positions(sentence):
    res = set()
    sent_split = sentence.split()
    for i, x in enumerate(sent_split):
        if x.strip() == ",":
            for j in range(4):
                res.add(i-j)
                res.add(i+j)
    # Need to return a list so it serializes with json
    return sorted([j for j in res if j >= 0 and j < len(sent_split)])

def create_single_item_trie(vocab, out_file=""):
    keys = []
    values = []
    for k in vocab:
        assert type(vocab[k]) is int
        keys.append(k)
        # Tries require list of item for the record trie
        values.append(tuple([vocab[k]]))
    fmt = "<l"
    trie = marisa_trie.RecordTrie(fmt, zip(keys, values))
    if out_file != "":
        trie.save(out_file)
    return trie

def create_trie(vocab, out_file = ""):
    trie = marisa_trie.Trie(vocab)
    if out_file != "":
        trie.save(out_file)
    return trie

def ngrams(words, n):
    return [ words[i:i+n] for i in range(len(words)-n+1) ]

def find_aliases_in_sentence_tag(sentence, all_aliases, max_alias_len, special_tag = "|||"):
    if len(all_aliases) == 0:
        return [], []
    used_aliases = []
    words_to_avoid = WORDS_TO_AVOID
    words_to_avoid.append(special_tag)
    sentence_split_raw = sentence.split()
    tags = pos_tag(sentence_split_raw)
    # find largest aliases first
    for n in range(max_alias_len+1, 0, -1):
        grams = ngrams(tags, n)
        j_st = -1
        j_end = n-1
        for gram in grams:
            j_st += 1
            j_end += 1
            gram_words = [g[0] for g in gram]
            gram_tags = [g[1] for g in gram]
            # If single word, must be noun (this will get rid of words like "the" or "as")
            if n == 1 and gram_tags[0] not in NOUNS:
                continue
            # For multi word aliases, make sure there is a noun in the phrase somewhere
            if n > 1 and not any(n in gram_tags for n in NOUNS):
                continue
            # If gram starts with stop word, move on because we'd rather try the one without
            # We also don't want punctuation words to be used at the beginning/end
            if gram_words[0] in words_to_avoid or gram_words[-1] in words_to_avoid or len(gram_words[0].translate(PUNC_TRANSLATION_TABLE).strip()) == 0\
                    or len(gram_words[-1].translate(PUNC_TRANSLATION_TABLE).strip()) == 0:
                continue
            gram_attempt = get_lnrm(" ".join(gram_words), strip=True, lower=True)
            # print("NOLRNSM", " ".join(gram_words), "-- GA", gram_attempt, j_st, "to", j_end, "-- in aliases --", gram_attempt in all_aliases)
            if gram_attempt in all_aliases:
                keep = True
                # We start from the largest n-grams and go down in size. This prevents us from adding an alias that is a subset of another.
                # For example: "Tell me about the mother on how I met you mother" will find "the mother" as alias and "mother". We want to
                # only take "the mother" and not "mother" as it's likely more descriptive of the real entity.
                for u_al in used_aliases:
                    u_j_st = u_al[1]
                    u_j_end = u_al[2]
                    if j_st < u_j_end and j_end > u_j_st:
                        keep = False
                        break
                if not keep:
                    continue
                # print("Adding", gram_attempt, j_st, j_end)
                used_aliases.append(tuple([gram_attempt, j_st, j_end]))
    # sort based on closeness to alias
    aliases_for_sorting = sorted(used_aliases, key=lambda elem: [elem[1], elem[2]])
    used_aliases = [a[0] for a in aliases_for_sorting]
    spans = [[a[1], a[2]] for a in aliases_for_sorting]
    return used_aliases, spans
