#!/usr/bin/env python
# -*- coding:utf8 -*-

from __future__ import print_function

import codecs
import re
import sys

from collections import defaultdict
from tqdm import tqdm

try:
    import cPickle as pickle
except:
    import pickle

sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)


RE_id_triple = re.compile(
    '<http://ja.dbpedia.org/resource/(.+?)>\s+<http://dbpedia.org/ontology/wikiPageID> "(.+?)"\^\^')


def generate_dbfile(alias_dict):
    sys.stderr.write('Loading alias dictionary ...\n')
    alias_dict = pickle.load(open(alias_dict))

    dbentries = []
    sys.stderr.write('Building DB file entry ...\n')
    for id_triple_line in tqdm(sys.stdin):
        id_triples = RE_id_triple.findall(id_triple_line)
        if id_triples:
            id_triple = id_triples[0]
            title = id_triple[0]
            wiki_id = int(id_triple[1])
            # Ignore redirection and aimai page.
            if (wiki_id in alias_dict['redirect']
               or wiki_id in alias_dict['aimai']):
                continue
            dbentries.append(list())
            # Add entity ID, name:Name, attr:WikipageID.
            dbentries[-1].append(u'{:d}\tname:Name:{:s}\tattr:WikipageID:{:d}'.format(
                title, title, wiki_id))
            # Add entity name:Alias
            if title in alias_dict['alias']:
                dbentries[-1].append(
                    '\t'.join([u'name:Alias:{:s}'.format(alias)
                               for alias
                               in list(alias_dict['alias'][title])])
                )
    return dbentries


def handle_argument(cmd_line_args=None):
    import argparse
    parser = argparse.ArgumentParser(usage='cat jawiki-YYYYMMDD-page-ids.ttl | %(prog)s [options]')
    parser.add_argument('alias_dict', metavar='REDIRECT', type=str,
                        help='Entity Alias dictionary. (pkl)')

    if cmd_line_args:
        args = parser.parse_args(cmd_line_args)
    else:
        args = parser.parse_args()

    dbentries = generate_dbfile(args.alias_dict)

    # Write Normalization DB file
    sys.stderr.write('Writing DB file entry ...\n')
    for dbentry in dbentries:
        sys.stdout.write('\t'.join(dbentry) + '\n')


if __name__ == '__main__':
    handle_argument()
