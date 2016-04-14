#!/usr/bin/python

import argparse
import json
import os
import sys
import time

CURL = '/usr/bin/curl'
OFFSET_KEY = '[iDisplayStart]'
OFFSET_STEP = 50
PRINT_INTERVAL = 10
# Keep retrying on error with backoff for about a hour.
ERROR_DELAY_SECS = [0, 1, 5, 10, 30] + [60] * 60

def flush(msg):
  print msg
  sys.stdout.flush()

def validate(jfile):
  try:
    j = json.load(open(jfile, 'r'))
  except ValueError:
    return False
  return 'aaData' in j

def download_data(args):
  with open(args.curl_tpl, 'r') as fp:
    curl_tpl = fp.read().strip()
  p = curl_tpl.find(OFFSET_KEY)
  assert p >= 0
  curl_prefix = curl_tpl[:p]
  curl_suffix = curl_tpl[p+len(OFFSET_KEY):]

  count = 0
  for offset in range(args.start, args.end, OFFSET_STEP):
    if count % PRINT_INTERVAL == 0:
      flush('downloaded %d files' % count)
    subdir = '%05d' % (count / args.files_per_folder)
    output_dir = '%s/%s' % (args.output_dir, subdir)
    if not os.path.isdir(output_dir):
      os.mkdir(output_dir)
    count += 1

    output_file = '%s/offset-%d.json' % (output_dir, offset)
    if os.path.isfile(output_file) and not args.overwrite:
      continue

    cmd = '%s --silent %s%d%s > %s' % (
        CURL, curl_prefix, offset, curl_suffix, output_file)
    ok = False
    for delay_sec in ERROR_DELAY_SECS:
      sleep_sec = args.sleep_sec + delay_sec
      if sleep_sec > 0:
        time.sleep(sleep_sec)

      if os.system(cmd) == 0:
        ok = validate(output_file)
        if ok:
          break
    assert ok, 'failed for offset %d: %s' % (offset, cmd)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--curl_tpl', required=True)
  parser.add_argument('--output_dir', required=True)
  parser.add_argument('--start', type=int, default=0)
  parser.add_argument('--end', type=int, default=10000)
  parser.add_argument('--sleep_sec', type=float, default=1)
  parser.add_argument('--files_per_folder', type=int, default=1000)
  parser.add_argument('--overwrite', type=bool, default=False)
  download_data(parser.parse_args())

if __name__ == '__main__':
  main()

