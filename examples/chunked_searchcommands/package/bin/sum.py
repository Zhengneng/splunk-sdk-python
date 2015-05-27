#!/usr/bin/env python
# coding=utf-8
#
# Copyright 2011-2015 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function, unicode_literals
import app

from splunklib.searchcommands import dispatch, ReportingCommand, Configuration, Option, validators
import sys


@Configuration()
class SumCommand(ReportingCommand):
    """ Computes the sum of a set of fields.

    ##Syntax

    .. code-block::
        sum total=<field> <field-list>

    ##Description:

    The total produced is sum(sum(fieldname, 1, n), 1, N) where n = number of fields, N = number of records.

    ##Example

    ..code-block::
        index = _internal | head 200 | sum total=lines linecount

    This example computes the total linecount in the first 200 records in the
    :code:`_internal index`.

    """
    total = Option(
        doc='''
        **Syntax:** **total=***<fieldname>*
        **Description:** Name of the field that will hold the computed sum''',
        require=True, validate=validators.Fieldname())

    @Configuration()
    def map(self, records):
        """ Computes sum(fieldname, 1, n) and stores the result in 'total' """
        self.logger.debug('SumCommand.map')
        total = 0.0
        for record in records:
            for fieldname in self.fieldnames:
                total += float(record[fieldname])
        yield {self.total: total}

    def reduce(self, records):
        """ Computes sum(total, 1, N) and stores the result in 'total' """
        self.logger.debug('SumCommand.reduce')
        total = 0.0
        for record in records:
            value = record[self.total]
            try:
                total += float(value)
            except ValueError:
                self.logger.debug('  could not convert value to float: %s', repr(value))
        yield {self.total: total}

    def __init__(self):
        ReportingCommand.__init__(self)
        self.record_count = long(0)

dispatch(SumCommand, sys.argv, sys.stdin, sys.stdout, __name__)
