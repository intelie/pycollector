#!/bin/bash
#
# File: rpm.sh
# Description: rpm generation script
#
# TODO: add support to source code downloading based on tag version.
#       for a while building from the current.
#
# 

TAR_PATH='/tmp'
RPM_PATH=`pwd`/rpmbuild


VERSION=`../../src/./pycollector --version`
RELEASE='1'


if [ "$?" -eq "0" ]; then
  echo "Removing previous builds..."
  rm -rf $RPM_PATH $TAR_PATH/pycollector-$VERSION

  echo "Setting build structure..."
  mkdir -p $RPM_PATH/{SOURCES,SPECS,BUILD,RPMS,SRPMS}
  echo "%_topdir %(echo $RPM_PATH)/" > ~/.rpmmacros

  echo "Generating tar.gz source code..."
  mkdir "$TAR_PATH/pycollector-$VERSION"
  cp -r ../../* $TAR_PATH/pycollector-$VERSION

  cd $TAR_PATH
  tar --exclude='*.pid' --exclude='*.pyc' --exclude-vcs -czvf pycollector-$VERSION.tar.gz pycollector-$VERSION
  cd -

  echo "Preparing source code..."
  cp $TAR_PATH/pycollector-$VERSION.tar.gz $RPM_PATH/SOURCES

  echo "Starting rpmbuild..."
  rpmbuild -bb -v --clean --define="ver $VERSION" --define="release $RELEASE" pycollector.spec

  if [ "$?" -eq "0" ]; then
    echo "RPM location:" `find $RPM_PATH/RPMS -name *.rpm`
  else
    echo "Fail."
    exit -1
  fi
fi
