#!/usr/bin/perl

use strict;
use warnings;

# Variables
my %zpool;
my %disks;
my @temp;
my $line = "";
my $pool = "";

# Get The ZPool Status
$zpool{'rawlist'} = `zpool list`;

# Get Individual ZPool Status
@temp = split('\n', $zpool{'rawlist'});

foreach $line (@temp) {
    if ( $line !~ /NAME/ ) {
        #               $1      $2      $3      $4      $5      $6      $7      $8      $9
        if ( $line =~ /(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+/ ) {
            printf("Found Data\n");

            $zpool{$1}{'Size'} = $2;
            $zpool{$1}{'Used'} = $3;
            $zpool{$1}{'Free'} = $4;
            $zpool{$1}{'Frag'} = $7;
            $zpool{$1}{'Cap'}  = $8;
            $zpool{$1}{'Health'} = $9;
        }
    }
}

# Debug Print
foreach $line ( keys %zpool) {
    printf("$line\n");
}
printf("\n");

# Loop over each pool
foreach $pool ( keys %zpool) {
    if ( $pool !~ /rawlist/ ) {
        $zpool{$pool}{'rawstatus'} = `zpool status $pool`;

        @temp = split('\n', $zpool{$pool}{'rawstatus'});

        foreach $line (@temp) {
            if ($line =~ /^\s+scan: scrub repaired (\d+B) in (\d\d:\d\d:\d\d) with (\d+) errors on ([a-zA-Z0-9:\s]+)/) {
                printf("Found scan match\n");
                $zpool{$pool}{'Repaired_Bytes'} = $1;
                $zpool{$pool}{'Repair_Time'}    = $2;
                $zpool{$pool}{'Scrub_Errors'}   = $3;
                $zpool{$pool}{'Scrub_Date'}     = $4;
           #} elsif ($line =~ /^\s+$pool\s+[a-zA-Z]+\s+(%d+)\s+(%d+)\s+(\d+)/) {
            } elsif ($line =~ /^\s+$pool\s+[a-zA-Z]+\s+(\d+)\s+(\d+)\s+(\d+)/) {
                printf("Found $pool match\n");
                $zpool{$pool}{'Read_Errors'}  = $1;
                $zpool{$pool}{'Write_Errors'} = $2;
                $zpool{$pool}{'CKSum_Errors'} = $3;
            }
        }
    }
}

#  HDD SMART Report Summary
#  Items to Collect
#  - Device
#  - Serial Number
#  - SMART Status
#  - Temp
#  - Power-On Time
#  - Start/Stop Count
#  - Spin Retry Count
#  - Realloced Sectors
#  - Realloced Events
#  - Current Pending Sectors
#  - Offline Uncorrectable Sectors
#  - UltraDMA CRC Errors
#  - Seek Error Health
#  - Last Test Age (Days)
#  - Last Test Type

# Print Storage Struct
# Send Email
# Get Smart Data
# Get ZFS Pool Data
#
