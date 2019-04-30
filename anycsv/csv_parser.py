#!/usr/bin/python
# -*- coding: utf-8 -*-


import csv
import os
import logging
import io
import requests

from anycsv import dialect
from anycsv.csv_model import Table
from anycsv import io_tools
from anycsv import exceptions
import gzip
import io

DEFAULT_ENCODING='utf-8'
ENC_PRIORITY=['magic', 'lib_chardet', 'header', 'default']

def reader(filename=None, url=None, content=None, skip_guess_encoding=False, delimiter=None, sniff_lines=100, max_file_size=-1, timeout=10):
    """

    :param filename:
    :param url:
    :param content: The content of a CSV file as a string
    :param skip_guess_encoding: If true, the parser uses utf-8
    :param delimiter:
    :param sniff_lines:
    :param timeout: url timeout in seconds
    :return:
    """
    logger = logging.getLogger(__name__)

    if not filename and not url and not content:
        raise exceptions.AnyCSVException('No CSV input specified')

    meta = sniff_metadata(filename, url, content, sniffLines=sniff_lines, timeout=timeout)
    table = Table(url=url, filename=filename)

    table.dialect = meta['dialect']
    if delimiter:
        table.delimiter = delimiter
        if 'delimiter' in table.dialect and table.dialect['delimiter'] != delimiter:
            logger.warning('The given delimiter differs from the guessed delimiter: ' + dialect['delimiter'])
    elif 'delimiter' in table.dialect:
        table.delimiter = table.dialect['delimiter']
    else:
        raise exceptions.NoDelimiterException('No delimiter detected')

    if 'quotechar' in table.dialect:
        table.quotechar = table.dialect['quotechar']

    if content:
        if max_file_size!=-1  and len(content)> max_file_size:
            raise exceptions.FileSizeException("Maximum file size exceeded {} > {} ".format(len(content), max_file_size))
        input = io.StringIO(content)
    elif filename and os.path.exists(filename):
        if filename[-3:] == '.gz':
            if max_file_size != -1 and os.stat(filename).st_size/0.4 > max_file_size: #assuming 40% compression / com/orig=0.4 -> orig = com/0.4
                raise exceptions.FileSizeException(
                    "Maximum file size exceeded {} > {} ".format(os.stat(filename).st_size, max_file_size))

            input = gzip.open(filename, 'r')
        else:
            if max_file_size != -1 and os.stat(filename).st_size  > max_file_size:
                raise exceptions.FileSizeException(
                    "Maximum file size exceeded {} > {} ".format(os.stat(filename).st_size, max_file_size))
            input = io.open(filename, 'r')
    elif url:
        input = URLHandle(url,max_file_size,timeout)
    else:
        raise exceptions.AnyCSVException('No CSV input specified')

    table.csvReader = EncodedCsvReader(input,
                                        delimiter=table.delimiter,
                                        quotechar=table.quotechar)

    return table


def sniff_metadata(fName= None, url=None, content=None, header=None, sniffLines=100, skip_guess_encoding=False, timeout=10):
    logger = logging.getLogger(__name__)
    id = url if url is not None else fName

    if not any([fName,url]) and not any([content, header]):
        #we need at least one of the three, so return empty results
        return {}

    if not any([content, header]) and any([fName, url]):
        res = io_tools.getContentAndHeader(fName=fName, url=url, download_dir="/tmp/", max_lines=sniffLines, timeout=timeout)
        content, header = res['content'], res['header']


    logger.debug('(%s) Extracting CSV meta data ', id)
    meta = extract_csv_meta(header=header, content=content, skip_guess_encoding=skip_guess_encoding)
    logger.debug("(%s) Meta %s ", id, meta)

    return meta


def extract_csv_meta(header, content=None, id='', skip_guess_encoding=False):
    logger = logging.getLogger(__name__)
    results = {'dialect': {}}

    # get dialect
    try:
        results['dialect'] = dialect.guessDialect(content.decode("utf-8"))
    except Exception as e:
        logger.warning('(%s)  %s',id, e.args)
        results['dialect']={}

    return results


class URLHandle:
    def __init__(self, url, max_file_size, timeout):
        self.url = url
        self.timeout = timeout
        self._init()
        self.max_file_size=max_file_size

    def _init(self):
        self._count = 0
        req = requests.get(self.url, timeout=self.timeout)
        self.input = req.iter_lines(chunk_size=1024)

    def seek(self, offset):
        if offset < self._count:
            self._init()
        while offset < self._count:
            next(self)

    def __iter__(self):
        return self

    def __next__(self):
        nxt = next(self.input)
        self._count += len(nxt)
        if self.max_file_size != -1 and self._count > self.max_file_size:
            raise exceptions.FileSizeException(
                "Maximum file size exceeded {} > {} ".format(self._count, self.max_file_size))

        return nxt.decode("utf-8")


class CsvReader:

    def __init__(self, f, reader, encoding):
        self.f = f
        self.reader = reader
        self.encoding = encoding
        self._start_line = 0
        self.line_num = 0

    def __iter__(self):
        return self

    def seek_line(self, line_number):
        if line_number < self.line_num:
            self.f.seek(0)
            self.line_num = 0
        self._start_line = line_number

    def _next(self):
        while self._start_line > self.line_num:
            next(self.reader)
            self.line_num += 1
        row = next(self.reader)
        self.line_num += 1
        return row


class EncodedCsvReader(CsvReader):
    def __init__(self, f, encoding="utf-8", delimiter="\t", quotechar="'", **kwds):
        if not quotechar:
            quotechar = "'"
        if not encoding:
            encoding = 'utf-8'
        if not delimiter:
            reader = csv.reader(f, quotechar=quotechar, **kwds)
        else:
            reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar,
                                     **kwds)
        CsvReader.__init__(self, f, reader, encoding)

    def __next__(self):
        return self._next()

class UnicodeReader(CsvReader):
    def __init__(self, f, delimiter="\t", quotechar="'", encoding='utf-8', errors='strict', **kwds):
        if not quotechar:
            quotechar = "'"
        if not encoding:
            encoding = 'utf-8'
        if not delimiter:
            reader = csv.reader(f, quotechar=quotechar, **kwds)
        else:
            reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar,
                                     **kwds)
        self.encoding_errors = errors
        CsvReader.__init__(self, f, reader, encoding)

    def __next__(self):
        row = self._next()
        encoding = self.encoding
        encoding_errors = self.encoding_errors
        float_ = float
        unicode_ = str
        return [(value if isinstance(value, float_) else
                  unicode_(value, encoding, encoding_errors)) for value in row]

