import argparse
import os

from textDNACSVGenerator import textDNA
from buildDataset import build


parser = argparse.ArgumentParser(description='textDNAGenerator <corpus_path (folder of texts OR folder of folders of texts)> <output_dir> <mode> (ngram or word_sequence)')
parser.add_argument('corpus_path', help='path to corpus to tag relative to the location of this script')
parser.add_argument('--output_dir', help='path to output folder relative to the location of this script', default=os.curdir)
parser.add_argument('mode', help='flag for indicating whether the dataset is n-gram or raw text (options: ngram OR word_sequence')
parser.add_argument('--folder_sequences', help='treats named subfolders rather than individual text files as sequences. for use with ngram mode ONLY.', action="store_true")
parser.add_argument('--name-prefix', '-n', help='name prefix of the output file')


if __name__ == '__main__':
    args = parser.parse_args()
    csv = textDNA(args)
    print csv
    with open(csv) as f:
        fname = build(f)
    os.remove(csv)
