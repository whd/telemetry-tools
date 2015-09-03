#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import shutil
import sys
import telemetry.util.s3 as s3util
from telemetry.telemetry_schema import TelemetrySchema

class FakeKey:
  name = None
  def __init__(self, n):
    self.name = n

class FakeBucket:
  def __init__(self, filenames):
    self.filenames = filenames

  def list(self, prefix='', delimiter=None):
    dirs = set()
    for f in self.filenames:
      if f[0:len(prefix)] == prefix:
        if delimiter is None:
          yield FakeKey(f)
        else:
          d = f[0:len(prefix)+1] + f[len(prefix) + 1:].split(delimiter)[0]
          dirs.add(d)
    if delimiter is None:
      return
    for d in dirs:
      yield FakeKey(d)

v2bucket = FakeBucket([
  "saved_session/Fennec/nightly/26.0a1/20130806030203.20131021.v2.log.8b30fadcf5b84df8b860bce47a23146a.lzma",
  "saved_session/Firefox/release/24.0/20130910160258.20131002.v2.log.264b07580df349678b1247d13ea2e6f3.lzma",
  "saved_session/Firefox/release/24.0/20130910160258.20131003.v2.log.25b53e7042c74188b08d71ce32e87237.lzma",
  "saved_session/Firefox/release/24.0/20130910160258.20131004.v2.log.29afd7a250154729bd53c20253f8af78.lzma",
  "saved_session/Firefox/release/24.0/20130910160258.20131005.v2.log.2bcf0e3a267d49f9a5256899f33ca484.lzma",
  "saved_session/Fennec/nightly/26.0a1/20130806030203.20131021.v2.log.BOGUS.lzma"])

v4bucket = FakeBucket([
  "20150903/telemetry/4/saved_session/Fennec/nightly/26.0a1/20130806030203/20150903051621.482_ip-172-31-16-184",
  "20150903/telemetry/4/saved_session/Firefox/release/24.0/20130910160257/20150903051622.482_ip-172-31-16-184",
  "20150903/telemetry/4/saved_session/Firefox/release/24.0/20130910160258/20150903051633.482_ip-172-31-16-184",
  "20150903/telemetry/4/saved_session/Firefox/release/24.0/20130910160258/20150903051644.482_ip-172-31-16-184",
  "20150903/telemetry/4/saved_session/Firefox/release/24.0/20130910160258/20150903051655.482_ip-172-31-16-184",
  "20150903/telemetry/4/saved_session/Fennec/nightly/26.0a1/20130806030203/20150903051626.482_ip-172-31-16-184"])

v4execbucket = FakeBucket([
  "20150901/20150901221519.541_ip-172-31-16-184",
  "20150901/20150901223019.579_ip-172-31-16-184",
  "20150901/20150901224519.623_ip-172-31-16-184",
  "20150902/20150902180014.543_ip-172-31-16-184",
  "20150902/20150902181514.593_ip-172-31-16-184",
  "20150902/20150902183014.640_ip-172-31-16-184",
  "20150903/20150903111503.204_ip-172-31-16-184",
  "20150903/20150903113003.306_ip-172-31-16-184",
  "20150903/20150903114503.387_ip-172-31-16-184",
  ])

def test_v2schema():
    schema_spec = {
      "version": 1,
      "dimensions": [
        {
          "field_name": "reason",
          "allowed_values": ["saved-session"]
        },
        {
          "field_name": "appName",
          "allowed_values": ["Firefox"]
        },
        {
          "field_name": "appUpdateChannel",
          "allowed_values": ["release"]
        },
        {
          "field_name": "appVersion",
          "allowed_values": ["24.0"]
        },
        {
          "field_name": "appBuildID",
          "allowed_values": ["20130910160258"]
        },
        {
          "field_name": "submission_date",
          "allowed_values": ["20131003","20131004"]
        }
      ]
    }

    schema = TelemetrySchema(schema_spec)

    found = set()
    for f in s3util.list_partitions(v2bucket, schema=schema, include_keys=True):
      found.add(f.name)

    assert(len(found) == 2)
    assert("saved_session/Firefox/release/24.0/20130910160258.20131003.v2.log.25b53e7042c74188b08d71ce32e87237.lzma" in found)
    assert("saved_session/Firefox/release/24.0/20130910160258.20131004.v2.log.29afd7a250154729bd53c20253f8af78.lzma" in found)

def test_v4schema():
    schema_spec = {
      "version": 2,
      "dimensions": [
        {
          "field_name": "submissionDate",
          "allowed_values": "20150903"
        },
        {
          "field_name": "sourceName",
          "allowed_values": "*"
        },
        {
          "field_name": "sourceVersion",
          "allowed_values": "4"
        },
        {
          "field_name": "docType",
          "allowed_values": ["saved-session"]
        },
        {
          "field_name": "appName",
          "allowed_values": ["Firefox"]
        },
        {
          "field_name": "appUpdateChannel",
          "allowed_values": ["release"]
        },
        {
          "field_name": "appVersion",
          "allowed_values": "24.0"
        },
        {
          "field_name": "appBuildId",
          "allowed_values": "20130910160258"
        }
      ]
    }
    schema = TelemetrySchema(schema_spec)

    found = set()
    for f in s3util.list_heka_partitions(v4bucket, schema=schema):
      found.add(f.name)

    assert(len(found) == 3)
    assert("20150903/telemetry/4/saved_session/Firefox/release/24.0/20130910160258/20150903051633.482_ip-172-31-16-184" in found)
    assert("20150903/telemetry/4/saved_session/Firefox/release/24.0/20130910160258/20150903051644.482_ip-172-31-16-184" in found)
    assert("20150903/telemetry/4/saved_session/Firefox/release/24.0/20130910160258/20150903051655.482_ip-172-31-16-184" in found)

def test_v4execschema():
    schema_spec = {
      "version": 2,
      "dimensions": [
        {
          "field_name": "submissionDate",
          "allowed_values": {"max": "20150901"}
        }
      ]
    }
    schema = TelemetrySchema(schema_spec)

    found = set()
    for f in s3util.list_heka_partitions(v4execbucket, schema=schema):
      found.add(f.name)

    assert(len(found) == 3)
    assert("20150901/20150901221519.541_ip-172-31-16-184" in found)
    assert("20150901/20150901223019.579_ip-172-31-16-184" in found)
    assert("20150901/20150901224519.623_ip-172-31-16-184" in found)

def main():
    test_v2schema()
    test_v4schema()
    test_v4execschema()
    return 0

if __name__ == "__main__":
    sys.exit(main())
