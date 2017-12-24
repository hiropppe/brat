#!/usr/bin/env python
# -*- coding: utf-8 -*-

import bz2
import gzip
import multiprocessing
import mwparserfromhell
import re
import sys

from collections import defaultdict
from functools import partial
from gensim.corpora import wikicorpus
from itertools import imap
from multiprocessing.pool import Pool
from tqdm import tqdm

try:
    import cPickle as pickle
except:
    import pickle

DEFAULT_IGNORED_NS = (
    'wikipedia:', 'category:', 'file:', 'portal:', 'template:', 'mediawiki:',
    'user:', 'help:', 'book:', 'draft:'
)

RE_parentheses_id2title = re.compile(ur"\((\d+),\d+,'?([^,']+)'?,[^\)]+\)")
RE_aimais = re.compile(ur'^[\*\+]+\s\[\[(.+?)[\|\]]', flags=re.DOTALL|re.MULTILINE)


def extract_title2id_from_sql(path):
    with gzip.GzipFile(path) as fd:
        return dict(((e[1], e[0])
                    for e in RE_parentheses_id2title.findall(fd.read().decode('utf8'))))


def extract_id2title_from_sql(path):
    with gzip.GzipFile(path) as fd:
        return dict(RE_parentheses_id2title.findall(fd.read().decode('utf8')))


class WikiDumpReader(object):
    def __init__(self, dump_file, ignored_ns=DEFAULT_IGNORED_NS):
        self._dump_file = dump_file
        self._ignored_ns = ignored_ns

        with bz2.BZ2File(self._dump_file) as f:
            self._language = re.search(r'xml:lang="(.*)"', f.readline()).group(1)

    @property
    def language(self):
        return self._language

    def __iter__(self):
        with bz2.BZ2File(self._dump_file) as f:
            for (title, wiki_text, wiki_id) in wikicorpus.extract_pages(f):
                if any(
                    [title.lower().startswith(ns) for ns in self._ignored_ns]
                ):
                    continue

                yield (unicode(title), unicode(wiki_text), unicode(wiki_id))


def _return_it(value):
    return value


def build_alias_dict(pages_articles_dump,
                     page_sql,
                     redirect_sql,
                     parallel=True,
                     pool_size=multiprocessing.cpu_count(),
                     chunk_size=100):
    dump_reader = WikiDumpReader(pages_articles_dump)

    sys.stderr.write('Building ID2TITLE dict ...\n')
    id2title = extract_id2title_from_sql(page_sql)
    sys.stderr.write('Building Redirection dict ...\n')
    rd_id2title = extract_id2title_from_sql(redirect_sql)

    if parallel:
        pool = Pool(pool_size)
        imap_func = partial(pool.imap_unordered, chunksize=chunk_size)
    else:
        imap_func = imap

    sys.stderr.write('Building alias dict ...\n')
    alias_dict = defaultdict(lambda: defaultdict(lambda: set()))
    for (title, wiki_txt, wiki_id) in tqdm(imap_func(_return_it, dump_reader)):
        title = title.replace(' ', '_')
        wiki_code = mwparserfromhell.parse(wiki_txt)
        wiki_id = int(wiki_id)
        # Add anchor text.
        for node in wiki_code.nodes:
            if isinstance(node, mwparserfromhell.nodes.Wikilink):
                e = unicode(node.title.strip_code()).replace(' ', '_')
                if node.text and not re.match(ur'^[\s\u3000]+$', node.text.strip_code()):
                    m = unicode(node.text.strip_code())
                    alias_dict['alias'][e].add(m)
        # Add redirection source.
        if wiki_id in rd_id2title:
            alias_dict['alias'][rd_id2title[wiki_id]].add(id2title[wiki_id])
            alias_dict['redirect'][wiki_id].add(id2title[wiki_id].replace(' ', '_'))
        # Add disumbiguation page entries.
        if wiki_code.contains(u'{{aimai}}'):
            for aimai in RE_aimais.findall(wiki_txt):
                aimai = aimai.replace(' ', '_')
                alias_dict['alias'][aimai].add(title)
                alias_dict['aimai'][wiki_id].add(title)
    return alias_dict


def handle_argument(cmd_line_args=None):
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('pages_articles_dump', metavar='PAGES_ARTICLES', type=str,
                        help='Wikipedia pages-articles XML DUMP (bz2)')
    parser.add_argument('page_sql', metavar='PAGE', type=str,
                        help='Wikipedia page SQL (gz)')
    parser.add_argument('redirect_sql', metavar='REDIRECT', type=str,
                        help='Wikipedia redirect SQL (gz)')
    parser.add_argument('alias_dict', metavar='OUT', type=str,
                        help='Output dictionay path')

    if cmd_line_args:
        args = parser.parse_args(cmd_line_args)
    else:
        args = parser.parse_args()

    dic = build_alias_dict(args.pages_articles_dump,
                           args.page_sql,
                           args.redirect_sql)

    sys.stderr.write('Saving dict ...\n')
    with open(args.alias_dict, 'w') as out:
        pickle.dump({'alias': dict(dic['alias']),
                     'redirect': dict(dic['redirect'])}, out)


if __name__ == '__main__':
    handle_argument()
