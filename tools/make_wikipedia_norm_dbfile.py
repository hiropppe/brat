#!/usr/bin/env python
# -*- coding:utf8 -*-

from __future__ import unicode_literals

import codecs
import gzip
import re
import sys

from tqdm import tqdm

try:
    import cPickle as pickle
except ImportError:
    import pickle

re_parentheses_id2title = re.compile(
    r"\((\d+),\d+,'?([^,']+)'?,[^\)]+\)")

sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)


def norm_title(title):
    return title.replace('_', ' ')


def get_pages(path):
    with gzip.GzipFile(path) as fd:
        return [(p[0], norm_title(p[1]))
                for p
                in re_parentheses_id2title.findall(fd.read().decode('utf8'))]


def generate_dbfile(page_sql_dump, alias_dict):
    sys.stderr.write('Loading alias dictionary ...\n')
    with open(alias_dict, mode='r') as fi:
        alias_dict = pickle.load(fi)

    dbentries = []
    sys.stderr.write('Building DB file entry ...\n')
    for wiki_id, title in tqdm(get_pages(page_sql_dump)):
        # Ignore Aimai Template and Redirection.
        if wiki_id in alias_dict['aimai'] or wiki_id in alias_dict['redirect']:
            continue

        dbentries.append(list())
        # Add entity ID, name:Name, attr:WikipageID.
        dbentries[-1].append('{:s}\tname:Name:{:s}\tattr:WikipageID:{:s}'.format(
            wiki_id, title, wiki_id))
        # Add name:Alias.
        if title in alias_dict['alias']:
            dbentries[-1].append(
                '\t'.join(['name:Alias:{:s}'.format(alias)
                           for alias
                           in list(alias_dict['alias'][title])])
            )

    # Write Normalization DB file
    sys.stderr.write('Writing DB file entry ...\n')
    for dbentry in dbentries:
        sys.stdout.write('\t'.join(dbentry) + '\n')

    sys.stderr.write('Done.\n')


def handle_argument(cmd_line_args=None):
    import argparse
    parser = argparse.ArgumentParser(usage='%(prog)s [options]')
    parser.add_argument(
        '--page_sql_dump', '-p', type=str, default=None, required=True,
        help='Wikipedia page sql dump file (gz)')
    parser.add_argument(
        '--alias_dict', '-a', type=str, default=None, required=True,
        help='Alias dictionay for entity page title (pkl)')

    args = parser.parse_args()

    generate_dbfile(args.page_sql_dump, args.alias_dict)


if __name__ == '__main__':
    handle_argument()
