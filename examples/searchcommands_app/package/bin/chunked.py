#!/usr/bin/env python

import app
import time

start_clock = time.clock()

import sys
import csv
import json
import re

import cStringIO as StringIO


def read_chunk(f):
    try:
        header = f.readline()
    except:
        return None

    if not header or len(header) == 0:
        return None

    m = re.match('chunked\s+1.0\s*,\s*(?P<metadata_length>\d+)\s*,\s*(?P<body_length>\d+)\s*\n', header)
    if m is None:
        print >>sys.stderr, 'Failed to parse transport header: %s' % header
        return None

    try:
        metadata_length = int(m.group('metadata_length'))
        body_length = int(m.group('body_length'))
    except:
        print >>sys.stderr, 'Failed to parse metadata or body length'
        return None

    print >>sys.stderr, 'READING CHUNK %d %d' % (metadata_length, body_length)

    try:
        metadata_buf = f.read(metadata_length)
        body = f.read(body_length)
    except Exception as e:
        print >>sys.stderr, 'Failed to read metadata or body: %s' % str(e)
        return None

    try:
        metadata = json.loads(metadata_buf)
    except:
        print >>sys.stderr, 'Failed to parse metadata JSON'
        return None

    return [metadata, body]


def write_chunk(f, metadata, body):
    metadata_buf = None
    if metadata:
        metadata_buf = json.dumps(metadata)
    f.write('chunked 1.0,%d,%d\n' % (len(metadata_buf) if metadata_buf else 0, len(body)))
    f.write(metadata_buf)
    f.write(body)
    f.flush()

if __name__ == "__main__":
    # getinfo exchange
    metadata, body = read_chunk(sys.stdin)
    capdata = {
        "type": "streaming",
    }

    modes = ['echo', 'sink', 'linesink', 'lineecho', 'where']
    mode = 'echo'
    if 'args' in metadata and type(metadata['args']) == list:
        for arg in metadata['args']:
            if arg in modes: mode = arg
        non_mode_args = list(set(metadata['args']).difference(set(modes)))
        if len(non_mode_args) > 0:
            capdata['selected_fields'] = non_mode_args

    write_chunk(sys.stdout, capdata, '')
    write_chunk(sys.stderr, capdata, '')
    sys.stderr.write('\n')

    if mode == 'noop':
        sys.exit(0)
    elif mode == 'sink':
        while True:
            ret = read_chunk(sys.stdin)
            if not ret: break
            metadata, body = ret
            write_chunk(sys.stdout, metadata, '')
    elif mode == 'echo':
        while True:
            ret = read_chunk(sys.stdin)
            if not ret: break
            metadata, body = ret
            write_chunk(sys.stdout, metadata, body)
    elif mode == 'linesink':
        while True:
            ret = read_chunk(sys.stdin)
            if not ret: break
            metadata, body = ret

            reader = csv.reader(body.splitlines(), dialect='excel')

            for line in reader: pass
            write_chunk(sys.stdout, metadata, '')
    elif mode == 'lineecho':
        while True:
            ret = read_chunk(sys.stdin)
            if not ret: break
            metadata, body = ret

            outbuf = StringIO.StringIO()

            reader = csv.reader(body.splitlines(), dialect='excel')
            writer = csv.writer(outbuf, dialect='excel')

            for line in reader:
                writer.writerow(line)

            write_chunk(sys.stdout, metadata, outbuf.getvalue())
    elif mode == 'where':
        while True:
            ret = read_chunk(sys.stdin)
            if not ret: break
            metadata, body = ret

            outbuf = StringIO.StringIO()

            reader = csv.reader(body.splitlines())
            try:
                header = reader.next()
                headerMap = dict(map(lambda (i,k):(k,i), enumerate(header)))
                value2idx = headerMap['value2']
            except:
                header = None

            if header:
                writer = csv.writer(outbuf)
                writer.writerow(header)

                for line in reader:
                    if float(line[value2idx]) < 0.10:
                        writer.writerow(line)

            write_chunk(sys.stdout, metadata, outbuf.getvalue())

    stop_clock = time.clock()
    print >>sys.stderr, "CPU TIME: %f" % (stop_clock - start_clock)
