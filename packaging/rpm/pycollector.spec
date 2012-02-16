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
Requires:     python >= 2.6


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
mkdir -p %{buildroot}/var/run/pycollector
mkdir -p %{buildroot}/var/log/pycollector

#bin files
cp src/pycollector src/__meta__.py %{buildroot}%{prefix}/bin/

#conf files
mv conf/conf.yaml.prod conf/conf.yaml
mv conf/daemon_conf.prod.py conf/daemon_conf.py
cp conf/conf.yaml conf/daemon_conf.py %{buildroot}%{prefix}/conf/
cp conf/conf.yaml conf/daemon_conf.py %{buildroot}%{prefix}/conf/

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
%dir %attr(755, %{pycollector_user}, %{pycollector_group}) %{prefix}/conf
%dir %attr(755, %{pycollector_user}, %{pycollector_group}) /var/run/pycollector
%dir %attr(755, %{pycollector_user}, %{pycollector_group}) /var/log/pycollector


%attr(777, %{pycollector_user}, %{pycollector_group}) %{prefix}/lib/**
%attr(644, %{pycollector_user}, %{pycollector_group}) %{prefix}/bin/__meta__.py
%attr(755, %{pycollector_user}, %{pycollector_group}) %{prefix}/bin/pycollector
%attr(644, %{pycollector_user}, %{pycollector_group}) %{prefix}/LICENSE
%attr(644, %{pycollector_user}, %{pycollector_group}) %{prefix}/CHANGELOG
%attr(644, %{pycollector_user}, %{pycollector_group}) %{prefix}/README
%config %attr(666, %{pycollector_user}, %{pycollector_group}) %{prefix}/conf/**
%attr(755, root, root) /etc/init.d/pycollector
