#!/usr/bin/python
# -*- coding: utf-8 -*-


class AnyCSVException(Exception):
    pass


class NoDelimiterException(AnyCSVException):
    pass

class FileSizeException(Exception):
    pass
