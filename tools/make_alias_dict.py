#!/usr/bin/env python
# -*- coding:utf8 -*-

from __future__ import unicode_literals

import bz2
import codecs
import gzip
import mwparserfromhell
import multiprocessing
import re
import sys
import unicodedata

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


re_parentheses_id2title = re.compile(
    r"\((\d+),\d+,'?([^,']+)'?,[^\)]+\)")
re_aimai_items = re.compile(
    r'^[\*\+]+\s\[\[(.+?)(?:\||\]\])', flags=re.DOTALL | re.MULTILINE)
re_title_brackets = re.compile(' \([^\)]+\)$')

DEFAULT_IGNORED_NS = (
    'wikipedia:', 'category:', 'file:', 'portal:', 'template:', 'mediawiki:',
    'user:', 'help:', 'book:', 'draft:'
)

sys.stdin = codecs.getreader('utf8')(sys.stdin)
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)


def extract_id2title_from_sql(path):
    with gzip.GzipFile(path) as fd:
        return dict(re_parentheses_id2title.findall(fd.read().decode('utf8')))


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
                # Don't use decode, type uncertain ... unicode? str? 
                yield (norm_title(unicode(title)), unicode(wiki_text), unicode(wiki_id))


def extract(value):
    title, wiki_text, wiki_id = value
    wiki_code = mwparserfromhell.parse(wiki_text)
    wikilinks = []
    for node in wiki_code.nodes:
        if isinstance(node, mwparserfromhell.nodes.Wikilink):
            node_title = norm_title(unicode(node.title.strip_code()))
            # Add only when anchor text not equals the title (= node.text has value).
            if node.text:
                node_text = norm_alias(unicode(node.text.strip_code()))
                if re.match(r'^[\s\u3000]+$', node_text) is None:
                    wikilinks.append((node_text, node_title))
    aimais = []
    if wiki_code.contains('{{aimai}}') or wiki_code.contains('{{Aimai}}'):
        for entity_title in re_aimai_items.findall(unicode(wiki_code)):
            aimais.append(norm_title(entity_title))
    return wiki_id, title, wikilinks, aimais


def norm_title(title):
    return title.replace('_', ' ')


def norm_alias(alias):
    alias = unicodedata.normalize('NFKC', alias)
    alias = re_title_brackets.sub('', alias)
    alias = alias.replace('_', ' ')
    alias = alias.replace('\t', ' ')
    alias = alias.replace('\n', '')
    return alias


def extract_alias_entity(dump_reader,
                         page_sql_dump,
                         redirect_sql_dump,
                         pool_size,
                         chunk_size=10):
    e2a = defaultdict(lambda: defaultdict(lambda: set()))

    sys.stderr.write('(Preprocess) Reading page.sql ...\n')
    id2title = extract_id2title_from_sql(page_sql_dump)
    sys.stderr.write('(Preprocess) Reading redirect.sql ...\n')
    rd_id2title = extract_id2title_from_sql(redirect_sql_dump)

    if pool_size > 1:
        pool = Pool(pool_size)
        imap_func = partial(pool.imap_unordered, chunksize=chunk_size)
    else:
        imap_func = imap

    sys.stderr.write('Building Alias dict ...\n')
    pbar = tqdm()
    for (wiki_id, title, wikilinks, aimais) in imap_func(extract, dump_reader):
        if wiki_id in id2title and wiki_id in rd_id2title:
            e2a['alias'][norm_title(rd_id2title[wiki_id])].add(norm_alias(id2title[wiki_id]))
            e2a['redirect'][wiki_id].add(id2title[wiki_id])

        for node_text, node_title in wikilinks:
            e2a['alias'][node_title].add(node_text)

        for disambi_title in aimais:
            e2a['alias'][disambi_title].add(norm_alias(title))
            e2a['aimai'][wiki_id].add(title)

        pbar.update(1)

    return e2a


def main(wiki_dump, page_sql_dump, redirect_sql_dump, out, pool_size):
    dump_reader = WikiDumpReader(wiki_dump)
    e2a = extract_alias_entity(dump_reader, page_sql_dump, redirect_sql_dump, pool_size=pool_size)
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
        '--pool_size', '-P', type=int, default=multiprocessing.cpu_count(), required=False,
        help='Process pool size.')
    parser.add_argument(
        'out', metavar='OUT', type=str,
        help='Output pickle dump file of dictionary.'
    )

    args = parser.parse_args()
    main(args.pages_article_dump,
         args.page_sql_dump,
         args.redirect_sql_dump,
         args.out,
         args.pool_size)
