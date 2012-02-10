#
# File: pycollector.spec
#
# Description: pycollector RPM spec file.
# For more details about rpm spec files, see -> http://www.rpm.org/max-rpm/ 
# 


%define pycollector_user   pycollector
%define pycollector_group  intelie


Name:         pycollector 
Version:      %{ver}
Release:      %{release}
Source:       %{name}-%{version}.tar.gz
Summary:      Collecting data should be simple 
License:      BSD
Group:        Applications/Utils
URL:          http://github.com/intelie/pycollector
Vendor:       Intelie
Packager:     Intelie
Prefix:       /opt/intelie/%{name}
BuildRoot:    /tmp/%{name}-%{version}-%{release}
AutoReqProv:  no
BuildArch:    noarch
Requires:     python >= 2.7


%description
Generic collector written in python 


%prep
%setup


%pre
groupadd %{pycollector_group}
useradd -g %{pycollector_group} %{pycollector_user} || :


%install
mkdir -m 755 -p %{buildroot}%{prefix}
mkdir -p %{buildroot}%{prefix}/bin 
mkdir -p %{buildroot}%{prefix}/lib
mkdir -p %{buildroot}%{prefix}/conf
mkdir -p %{buildroot}%{prefix}/logs
mkdir -p %{buildroot}/etc/init.d

#bin files
cp src/pycollector src/__meta__.py %{buildroot}%{prefix}/bin/

#conf files
cp -r conf/* %{buildroot}%{prefix}/conf/

#lib files
cp CHANGELOG LICENSE README %{buildroot}%{prefix}
cp -r src/* %{buildroot}%{prefix}/lib/
rm -rf %{buildroot}%{prefix}/lib/pycollector

#changing paths
sed -i "s/src/lib/g" %{buildroot}%{prefix}/bin/__meta__.py

#services symlinks
ln -s %{prefix}/bin/pycollector %{buildroot}/etc/init.d


%post


%preun
sudo service pycollector --stop || :


%postun


%clean
rm -rf %{buildroot}


%files
%dir %attr(755, %{pycollector_user}, %{pycollector_group}) %{prefix}/bin
%dir %attr(755, %{pycollector_user}, %{pycollector_group}) %{prefix}/lib
%dir %attr(755, %{pycollector_user}, %{pycollector_group}) %{prefix}/logs
%dir %attr(755, %{pycollector_user}, %{pycollector_group}) %{prefix}/conf
%attr(777, %{pycollector_user}, %{pycollector_group}) %{prefix}/lib/**
%attr(644, %{pycollector_user}, %{pycollector_group}) %{prefix}/bin/__meta__.py
%attr(755, %{pycollector_user}, %{pycollector_group}) %{prefix}/bin/pycollector
%attr(644, %{pycollector_user}, %{pycollector_group}) %{prefix}/LICENSE
%attr(644, %{pycollector_user}, %{pycollector_group}) %{prefix}/CHANGELOG
%attr(644, %{pycollector_user}, %{pycollector_group}) %{prefix}/README
%config %attr(666, %{pycollector_user}, %{pycollector_group}) %{prefix}/conf/**
%attr(755, root, root) /etc/init.d/pycollector
