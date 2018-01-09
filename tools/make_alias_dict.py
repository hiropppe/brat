#!/usr/bin/env python
# -*- coding:utf8 -*-

import bz2
import codecs
import gzip
import mwparserfromhell
import multiprocessing
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
except ImportError:
    import pickle


RE_parentheses_id2title = re.compile(
    ur"\((\d+),\d+,'?([^,']+)'?,[^\)]+\)")
RE_aimai_items = re.compile(
    ur'^[\*\+]+\s\[\[(.+?)(?:\||\]\])', flags=re.DOTALL | re.MULTILINE)

DEFAULT_IGNORED_NS = (
    'wikipedia:', 'category:', 'file:', 'portal:', 'template:', 'mediawiki:',
    'user:', 'help:', 'book:', 'draft:'
)

sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)


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
                if any([title.lower().startswith(ns) for ns in self._ignored_ns]):
                    continue
                yield (unicode(title), unicode(wiki_text), unicode(wiki_id))


def _return_it(value):
    return value


def extract_alias_entity(dump_reader,
                         page_sql_dump,
                         redirect_sql_dump,
                         parallel=False,
                         pool_size=multiprocessing.cpu_count(),
                         chunk_size=100):
    e2a = defaultdict(lambda: defaultdict(lambda: set()))

    sys.stderr.write('(Preprocess) Building Title dict ...\n')
    id2title = extract_id2title_from_sql(page_sql_dump)
    sys.stderr.write('(Preprocess) Building Redirection dict ...\n')
    rd_id2title = extract_id2title_from_sql(redirect_sql_dump)

    if parallel:
        pool = Pool(pool_size)
        imap_func = partial(pool.imap_unordered, chunksize=chunk_size)
    else:
        imap_func = imap

    sys.stderr.write('Building Alias dict ...\n')
    for (title, wiki_txt, wiki_id) in tqdm(imap_func(_return_it, dump_reader)):
        # redirect
        if wiki_id in id2title and wiki_id in rd_id2title:
            e2a['alias'][rd_id2title[wiki_id]].add(id2title[wiki_id].replace(' ', '_'))
            e2a['redirect'][wiki_id].add(id2title[wiki_id].replace(' ', '_'))

        title = title.replace(' ', '_')
        wiki_code = mwparserfromhell.parse(wiki_txt)
        # anchor
        for node in wiki_code.nodes:
            if isinstance(node, mwparserfromhell.nodes.Wikilink):
                e = unicode(node.title.strip_code()).replace(' ', '_')
                # Add only when anchor text not equals the title (= node.text has value).
                if node.text and re.match(ur'^[\s\u3000]+$', node.text.strip_code()) is None:
                    anchor = unicode(node.text.strip_code())
                    e2a['alias'][e.replace(' ', '_')].add(anchor)

        # aimai
        if wiki_code.contains('{{Aimai}}'):
            for e in RE_aimai_items.findall(unicode(wiki_code)):
                e2a['alias'][e.replace(' ', '_')].add(title)
                e2a['aimai'][wiki_id].add(title)

    return e2a


def main(wiki_dump, page_sql_dump, redirect_sql_dump, out):
    dump_reader = WikiDumpReader(wiki_dump)
    e2a = extract_alias_entity(dump_reader, page_sql_dump, redirect_sql_dump)
    with open(out, mode='w') as fo:
        pickle.dump({'alias': dict(e2a['alias']),
                     'redirect': dict(e2a['redirect']),
                     'aimai': dict(e2a['aimai'])}, fo)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--pages_article_dump', '-a', type=str, default=None, required=True,
        help='Wikipedia pages-articles dump file (bz2)')
    parser.add_argument(
        '--page_sql_dump', '-p', type=str, default=None, required=True,
        help='Wikipedia page sql dump file (gz)')
    parser.add_argument(
        '--redirect_sql_dump', '-r', type=str, default=None, required=True,
        help='Wikipedia redirect sql dump file (gz)')
    parser.add_argument(
        'out', metavar='OUT', type=str,
        help='Output pickle dump file of dictionary.'
    )

    args = parser.parse_args()
    main(args.pages_article_dump,
         args.page_sql_dump,
         args.redirect_sql_dump,
         args.out)
