#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import sys
import telemetry.util.s3 as s3util
from telemetry.telemetry_schema import TelemetrySchema
import json
import itertools
import time

schema_spec = json.loads("""{
  "version": 2,
  "dimensions": [
    {
      "field_name": "submissionDate",
      "allowed_values": "*"
    },
    {
      "field_name": "sourceName",
      "allowed_values": "*"
    },
    {
      "field_name": "sourceVersion",
      "allowed_values": "*"
    },
    {
      "field_name": "docType",
      "allowed_values": [
        "idle-daily",
        "saved-session",
        "android-anr-report",
        "ftu",
        "loop",
        "flash-video",
        "main",
        "activation",
        "deletion",
        "crash",
        "uitour-tag"
      ]
    },
    {
      "field_name": "appName",
      "allowed_values": [
        "Firefox",
        "Fennec",
        "Thunderbird",
        "FirefoxOS",
        "B2G"
      ]
    },
    {
      "field_name": "appUpdateChannel",
      "allowed_values": [
        "default",
        "nightly",
        "aurora",
        "beta",
        "release",
        "esr"
      ]
    },
    {
      "field_name": "appVersion",
      "allowed_values": "*"
    },
    {
      "field_name": "appBuildId",
      "allowed_values": "*"
    },
    {
      "field_name": "telemetryEnabled",
      "allowed_values": [
        "true",
        "false"
      ]
    },
    {
      "field_name": "sampleId",
      "allowed_values": "*"
    }
  ]
}
""")

submission_dates = [
    ["20151111"], ["20151111", "20151112", "20151113", "20151114", "20151115"]]
source_names = ["*", ["telemetry"]]
source_versions = ["*", ["4"]]
doc_types = [
    ["idle-daily", "saved-session", "android-anr-report", "ftu",
     "loop", "flash-video", "main",
     "activation", "deletion", "crash", "uitour-tag"],
    ["main"],
    ["saved-session", "main"]]
app_names = [
    ["Firefox", "Fennec", "Thunderbird", "FirefoxOS", "B2G"],
    ["Firefox"]]
app_update_channels = [
    "*",
    ["release", "beta"],
    ["release"],
    ["beta"]]
app_versions = ["*", ["42.0"]]
app_build_ids = [
    "*",
    ["20151029151421"],
    # ["20130910201120",
    #  "20150921151815",
    #  "20150928102225",
    #  "20151001142456",
    #  "20151005144425"]
]
telemetry_enabled = ["*", ["true", "false"]]
sample_ids = ["*", ["42"], ["0", "1", "2", "3", "4"]]


def test_list_perf(schema):
    t1 = time.time()
    schema = TelemetrySchema(schema_spec)
    l = s3util.Loader("/tmp", "net-mozaws-prod-us-west-2-pipeline-data")

    found = set()
    for f in s3util.list_heka_partitions(
            l.bucket, schema=schema, prefix="telemetry-release/"):
        found.add(f.name)
    t2 = time.time()
    print("found {} objects".format(len(found)))
    print("schema={}\nTime={}\n".format(json.dumps(schema_spec), t2 - t1))


def main():
    for f in itertools.product(
        submission_dates,
        source_names,
        source_versions,
        doc_types,
        app_names,
        app_update_channels,
        app_versions,
        app_build_ids,
        telemetry_enabled,
            sample_ids):
        for i, v in enumerate(f):
            schema_spec["dimensions"][i]["allowed_values"] = v
        test_list_perf(schema_spec)
    return 0

if __name__ == "__main__":
    sys.exit(main())
