#Copyright (c) 2016, Danielle Albers Szafir, Deidre Stuffer, Yusef Sohail, & Michael Gleicher
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
#
#1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
#
#2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
#
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

### Python code to do data processing and management
import csv
import sqlite3
import datetime
import json
import sys
import os
import tempfile

defaults = ["sequence", "word", "rank", "frequency", "groupedFrequency"]

lastHeader = None


# Aggregate function to create a list of sequences
class ListSeqs:
    def __init__(self):
        self.seqs = []

    def step(self, value):
        self.seqs.append(value)

    def finalize(self):
        return self.seqs


# Rearrange data entry to conform to the expected order
def rearrange(row, seqId, header):
    # datapoint is: (word, seqence_id, rank, frequency, groupedFrequency)
    # + added 'traits' ('Count' in case of word_sequence)
    datapoint = (row[header.index('word')].strip(), seqId, row[header.index('rank')], 0, "")
    for i in range(0, len(header)):
        if not header[i] in defaults:
            datapoint += (row[i], )
    return datapoint


# Parse a formatted N-Gram file
def parseNgramFile(csvfile, db_file_name):
    header = []  # fields in database (data extracted, rank, etc.)
    seqs = []  # list of (aggregated) text files
    data = []
    seqIdx = 0

    db = sqlite3.connect(db_file_name)
    db.create_aggregate("listseqs", 1, ListSeqs)
    c = db.cursor()

    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:  # do for header (not the data)
        if len(header) == 0:
            # Fetch the header
            header = map(str.strip, row)
            seqIdx = row.index('sequence')

            # Add any missing elements to the word database
            c.execute("DROP TABLE IF EXISTS sequences")
            c.execute("DROP TABLE IF EXISTS words")
            c.execute("CREATE TABLE IF NOT EXISTS sequences (id INTEGER PRIMARY KEY, seqName STRING, seqOrder INTEGER)")
            createWordTable = "CREATE TABLE IF NOT EXISTS words (word STRING, seqId INTEGER, rank INTEGER, frequency INTEGER, groupedFrequency STRING"
            for trait in header:
                if not (trait.strip() in defaults):
                    key = trait.split(" ")
                    createWordTable += ", " + " ".join(key[1:]) + " " + key[0]
                    print "trait is " + trait
            createWordTable += ");"

            c.execute(createWordTable)
            c.execute('''CREATE INDEX IF NOT EXISTS idx1 on words(word)''')
            c.execute('''CREATE INDEX IF NOT EXISTS idx2 on words(seqId)''')

            db.commit()

        else:  # do for data (not the header)
            if not row[seqIdx].strip() in seqs:
                # add data to sequences
                c.execute("INSERT INTO sequences VALUES (?,?,?)", (str(len(seqs)), row[seqIdx].strip(), str(len(seqs))))
                seqs.append(row[seqIdx].strip())

            # Create the value tuple
            data.append(rearrange(row, seqs.index(row[seqIdx]), header))

    # Batch insert
    queryString = "(" + ("?, " * (len(row) + 2))[0:-2] + ")"
    c.executemany('INSERT INTO words VALUES ' + queryString, data)

    # Derive the secondary params (frequency and groupedFrequency)
    # Same as function composeSecondaryData
    # Return a list of sequence ids for every word that occurs in the corpus
    c.execute("SELECT word, group_concat(seqId) FROM words GROUP BY word")
    for word in c.fetchall():
        gf = "0" * len(seqs)
        for s in str.split(str(word[1]), ','):
            idx = int(s)
            gf = gf[:idx] + gf[idx:idx+1].replace('0', '1') + gf[idx+1:]
        frequency = gf.count('1')
        c.execute("UPDATE words SET frequency=?, groupedFrequency=? WHERE word=?", (frequency, "'"+gf+"'", word[0]))

    db.commit()
    db.close()
    return (header, seqs)


# Build the secondary data
# Function is not used in this script
def composeSecondaryData(csvfile, numSeqs, db_file_name):
    # Compute the grouped frequency lists
    db = sqlite3.connect(db_file_name)
    c = db.cursor()
    c.execute("SELECT word, group_concat(seqId) FROM words GROUP BY word")

    # Derive the frequencies
    for word in c.fetchall():
        gf = "0" * numSeqs
        for s in str.split(str(word[1]), ','):
            idx = int(s)
            gf = gf[:idx] + gf[idx:idx+1].replace('0', '1') + gf[idx+1:]
        frequency = gf.count('1')
        c.execute("UPDATE words SET frequency=?, groupedFrequency=? WHERE word=?", (frequency, "'"+gf+"'", word[0]))

    # Push updates to the table
    db.commit()
    db.close()


# Pull down data from the database
def fetchDataForDisplay(csvfile, header, orderBy, db_file_name):
    db = sqlite3.connect(db_file_name)
    c = db.cursor()
    c.execute("SELECT * FROM words JOIN sequences WHERE words.seqId = sequences.id ORDER BY " + "rank" + " ASC")   #no idea if this works. shrug.
    tempData = c.fetchall()
    data = {}
    for d in tempData:
        if not (d[-3] in data):
            data[d[-3]] = []
        data[d[-3]].append(list(d))
    c.execute("SELECT id, seqName, seqOrder, COUNT(word) as length FROM sequences, words WHERE id = seqId GROUP BY id ORDER BY id ")
    seqs = c.fetchall()

    # Determine the local extrema
    maxs = []
    mins = []
    for i in header:
        key = str.split(i, " ")[-1]
        print i
        try:
            c.execute("SELECT MAX("+key+"), MIN("+key+") FROM words")
            extrema = c.fetchall()
            maxs.append(extrema[0][0])
            mins.append(extrema[0][1])
        except sqlite3.Error as e:
            print "failed on " + key
            maxs.append(0)
            mins.append(0)
    #seqCounts = c.fetchall()
    db.close()
    return [maxs, mins, seqs, data]


# Update the header to reflect the database schema
def generateSchema(header):
    baseHeader = ["STRING word", "INT seqId", "INT rank", "INT frequency", "STRING groupedFrequency"]
    modHeader = header[:]
    for column in defaults:
        if column in modHeader:
            modHeader.remove(column)
    baseHeader.extend(modHeader)
    baseHeader.extend(["INT seqId", "STRING seqName", "INT seqOrder"])
    return baseHeader


# Construct dataset -- takes in a file pointer, outputs a JSON dataset sorted according to the default params
def build(fileptr):
    db_file = tempfile.NamedTemporaryFile(delete=False)
    # Parse data from file
    startTime = datetime.datetime.now();
    if (fileptr.name[-3:] == "zip"):
        (header, seqs) = parseTextFile(fileptr)
    else:
        (header, seqs) = parseNgramFile(fileptr, db_file.name)

    print("parsed file in " + str(datetime.datetime.now() - startTime))
    startTime = datetime.datetime.now()

    # Add supplemental frequency data
    #rawData = composeSecondaryData(len(seqs))
    dataset = [generateSchema(header)]
    dataset.extend(fetchDataForDisplay(fileptr, dataset[0], "rank", db_file.name))
    print "formatted data in " + str(datetime.datetime.now() - startTime)

    # Write the JSONified dataset to file
    fname = os.path.splitext(os.path.basename(fileptr.name))[0]
    jsonfile = "data/json/{}.json".format(fname)
    with open(jsonfile, 'w') as outfile:
        json.dump(dataset, outfile, indent=4)

    db_file.close()
    os.remove(db_file.name)

    return fname


def updateList(fname):
    flag = "<ul>My Data:</ul>"
    with open("app/templates/list.html", "r") as f:
        lines = f.readlines()

        # Find the right line
        for idx in range(0, len(lines)):
            l = lines[idx]
            if (l.strip()==flag):
                lines.insert(idx+1, "<ul><a href=\"viewer.html?file=" + fname + "\">" + fname + "</a></ul>\n")
                break

        # Write the updated file
        f = open("app/templates/list.html", "w")
        lines = "".join(lines)
        f.write(lines)
        f.close()


# This function is not used
def buildClientData(orderBy):
    startTime = datetime.datetime.now();
    dataset = [generateSchema(['sequence', 'word', 'rank', 'INTEGER count'])]
    dataset.extend(fetchDataForDisplay(dataset[0], orderBy))
    return {"data": dataset}

if __name__ == '__main__':
    csv_path = sys.argv[1]

    # check to verify csv_path is to a file
    if not os.path.isfile(os.path.abspath(csv_path)):
        print "Path {} not valid.\nUsage: {} [csv_path]".format(csv_path,
                                                                __file__)
        sys.exit(0)

    with open(os.path.abspath(csv_path)) as f:
        fname = build(f)

    # Put the dataset in the list (referenced by the filename)
    updateList(fname)
