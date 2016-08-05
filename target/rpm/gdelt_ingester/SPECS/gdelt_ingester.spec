Name: gdelt_ingester
Version: 0.0.1
Release: SNAPSHOT20160805024603
Summary: gdelt_ingester
License: 2016 Incadence Strategic Solutions
Group: Coalesce
Packager: Bryan Moore
autoprov: yes
autoreq: yes
Prefix: /opt/gdelt_ingest
BuildRoot: /home/bmoore/workspace/gdelt_ingester/target/rpm/gdelt_ingester/buildroot

%description

%install
if [ -e $RPM_BUILD_ROOT ];
then
  mv /home/bmoore/workspace/gdelt_ingester/target/rpm/gdelt_ingester/tmp-buildroot/* $RPM_BUILD_ROOT
else
  mv /home/bmoore/workspace/gdelt_ingester/target/rpm/gdelt_ingester/tmp-buildroot $RPM_BUILD_ROOT
fi

%files
%defattr(0640,cloud,cloud,0750)
 "/opt/gdelt_ingest/lib"
 "/opt/gdelt_ingest/conf"
%attr(750,cloud,cloud) "/opt/gdelt_ingest/bin"
 "/opt/gdelt_ingest/data"
