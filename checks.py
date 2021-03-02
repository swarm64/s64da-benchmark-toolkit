#!/usr/bin/env python3

from shutil import which
import sys


def check_program_exists(program):
    if which(program) is None:
        print(
            "ERROR: Could not find {}. Is it installed? Is PATH setup properly?".format(
                program
            )
        )
        sys.exit()
