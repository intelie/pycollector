#
# File: package.sh
# Description: shortcut to waf packaging.
#

rm -r build
mkdir build
cp -r src build/
cp -r conf build/
mv build/src build/bin
find build -name '*.pyc' -exec rm {} \;
find build -name '*.log' -exec rm {} \;


