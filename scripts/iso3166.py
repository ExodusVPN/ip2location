#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json

SCRIPT_DIR   = os.path.dirname(os.path.realpath(__file__))
PROJECT_ROOT = os.path.realpath(os.path.join(SCRIPT_DIR, "../"))

# ISO-3166-Countries-with-Regional-Codes
# https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes/blob/master/all/all.csv
ISO3166_DATA = os.path.join(SCRIPT_DIR, "iso3166_all.json")

def gen_cc():
    data = json.loads(open(ISO3166_DATA, "r").read())
    items = list(map(lambda elem: (elem["alpha-2"], elem["name"]), data))
    items = sorted(items, key=lambda item: item[0])
    # CodeGen
    body = ""
    for i in range(len(items)):
        item = items[i]
        if i % 1 == 0:
            body += "\n    "
        body += "(\"%s\", \"%s\"), " % item
    body += "\n"
    code = "\npub static COUNTRY_CODES: [(&'static str, &'static str); %d] = [%s];\n" % ( len(items), body)
    print(code)


def main():
    gen_cc()


if __name__ == '__main__':
    main()
