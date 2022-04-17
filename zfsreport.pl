#!/usr/bin/perl

use strict;
use warnings;

# Variables
my %zpool;
my %disks;

# Get The ZPool Status
$zpool{'rawlist'} = `zpool list`;

# Get Individual ZPool Status
my @temp = split('\n', $zpool{'rawlist'});
my $line = "";

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

foreach $line ( keys %zpool) {
    printf("$line\n");
}

# Items to Collect

#  - Name
#  - Status
#  - Read Errors
#  - Write Errors
#  - Checksum Error
#  - % Used
#  - Scrub Repaied Bytes
#  - Scrub Errors
#  - Last Scrub Age
#  - Last Scrub Time
#
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
