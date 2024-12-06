#!/usr/bin/python3
import os
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    os.system("gradle :superopt:run --args='FindWeTuneRules'")
