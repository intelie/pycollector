#!/usr/bin/env python
#
# File: wscript
# Description: read by waf to generate a new package
#
# How to use:
# $ waf distclean configure build dist distclean
#

APPNAME = "log-collector"

top = "."
out = "./build"


def configure(ctx):
    pass


def build(bld):
    bld(rule='find .. -name "*.pyc" -delete')
    bld(rule='mkdir bin')
    bld(rule='cp -r ../src .')
    bld(rule='mv src/collectord bin')
    bld(rule='cp -r ../conf .')
    bld(rule='cp -r ../tests .')


def dist(dst):
    to_include = ['build/src/**', 'build/conf/**', 'build/tests/**', 'build/bin/**']
    dst.files = dst.path.ant_glob(to_include)
