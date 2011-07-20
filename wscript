#!/usr/bin/env python
#
# File: wscript
# Description: read by waf to generate a new package
#
# How to use:
# $ waf distclean configure build dist distclean
#

APPNAME = "log-collector"
VERSION = "0.1.2"

top = "."
out = "./build"


def configure(ctx):
    pass


def build(bld):
    bld(rule='find .. -name "*.pyc" -delete')
    bld(rule='mkdir bin')
    bld(rule='cp -r ../src .')
    bld(rule='mv src/logcollectord bin')
    bld(rule='cp ../README .')
    bld(rule='cp -r ../conf .')


def dist(dst):
    to_include = ['build/src/**', 'build/conf/**', 'build/bin/**', 'build/README']
    dst.files = dst.path.ant_glob(to_include)
