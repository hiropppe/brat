#!/usr/bin/env python
# -*- coding:utf8 -*-

from __future__ import print_function

import codecs
import re
import sys

from tqdm import tqdm

try:
    import cPickle as pickle
except ImportError:
    import pickle

sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

RE_id_triple = re.compile(
    ur'<http://ja.dbpedia.org/resource/(.+?)>\s+<http://dbpedia.org/ontology/wikiPageID> "(.+?)"\^\^')


def generate_dbfile(alias_dict):
    sys.stderr.write('Loading alias dictionary ...\n')
    with open(alias_dict, mode='r') as fi:
        alias_dict = pickle.load(fi)

    dbentries = []
    sys.stderr.write('Building DB file entry ...\n')
    for id_triple_line in tqdm(sys.stdin):
        id_triples = RE_id_triple.findall(id_triple_line)
        if id_triples:
            id_triple = id_triples[0]
            wiki_title = id_triple[0]
            wiki_id = id_triple[1]
            # Ignore Aimai Template and Redirection.
            if wiki_id in alias_dict['aimai'] or wiki_id in alias_dict['redirect']:
                continue

            dbentries.append(list())
            # Add entity ID, name:Name, attr:WikipageID.
            dbentries[-1].append(u'{:s}\tname:Name:{:s}\tattr:WikipageID:{:s}'.format(
                wiki_title, wiki_title, wiki_id))
            # Add name:Alias.
            if wiki_title in alias_dict['alias']:
                dbentries[-1].append(
                    '\t'.join([u'name:Alias:{:s}'.format(alias)
                               for alias
                               in list(alias_dict['alias'][wiki_title])])
                    )

    # Write Normalization DB file
    sys.stderr.write('Writing DB file entry ...\n')
    for dbentry in dbentries:
        sys.stdout.write('\t'.join(dbentry) + '\n')

    sys.stderr.write('Done.\n')


def handle_argument(cmd_line_args=None):
    import argparse
    parser = argparse.ArgumentParser(usage='cat <path_to_dbpedia_page_ids_ttl> | %(prog)s [options]')
    parser.add_argument('--alias_dict', '-a', type=str, default=None, required=True,
                        help='Alias dictionay for entity page title (pkl)')

    args = parser.parse_args()

    generate_dbfile(args.alias_dict)


if __name__ == '__main__':
    handle_argument()
