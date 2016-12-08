# -*- coding: utf-8 -*-
import nltk
from nltk.corpus import stopwords
import xml.etree.ElementTree as et
import re
from nltk import stem
import csv
import os
import HTMLParser

from multiprocessing import Pool
from functools import partial
import datetime
import Util
import traceback
import sys
from compiler.ast import flatten

debug=False

def _pickle_method(method):
    func_name = method.im_func.__name__
    obj = method.im_self
    cls = method.im_class
    if func_name.startswith('__') and not func_name.endswith('__'): #deal with mangled names
        cls_name = cls.__name__.lstrip('_')
        func_name = '_' + cls_name + func_name
    return _unpickle_method, (func_name, obj, cls)

def _unpickle_method(func_name, obj, cls):
    for cls in cls.__mro__:
        try:
            func = cls.__dict__[func_name]
        except KeyError:
		pass
        else:
		break
    return func.__get__(obj, cls)

import copy_reg
import types
copy_reg.pickle(types.MethodType, _pickle_method, _unpickle_method)

category = sys.argv[1].lower()

class Preprocessor(object):
    def __init__(self,work_dir):
        self.html_parser = HTMLParser.HTMLParser()
        self.work_dir = work_dir
        # get stopwords from nltk
        self.stops = set(stopwords.words('english'))
        # get stopsowords from file
        stops_2 = open("stopwords","rb")
        for stop in stops_2:
            self.stops.add(stop.strip())
        tokenized_stops=set([])
        # tokenize stop words:
        for stop in self.stops:
            tokenized_stops = tokenized_stops | set(nltk.word_tokenize(stop))
        self.stops = tokenized_stops | self.stops
        self.stemmer = stem.PorterStemmer()

    def preprocess(self,filename):
        #print datetime.datetime.now()
        print "start : " + filename
        preprocessed_list = []
        with open("".join(filename), "rb") as splited_post_file:

            for row in splited_post_file:
                try:
                    # xml parse
                    e = et.fromstring(row)
                    # skip if post is not question
                    if not e.get("PostTypeId") == "1":
                        continue
                    # tags
                    tags = e.get("Tags")
                    # skip if tag does not have category
                    if not "<" + category + ">" in tags:
                        continue

                    # id
                    id = e.get("Id")

                    # skip if img is included
                    body = e.get("Body")
                    if "<img>" in body:
                        continue

                    # preprocess tag
                    tag_list = tags.strip("<").strip(">").split("><")
                    tag_list.remove(category)
                    tag_list  = [unicode(tag).encode("utf-8") for tag in tag_list]


                    # preprocess title
                    title = e.get("Title")
                    preprocessed_title = []
                    for a in self.preprocess_line(title):
                        try:
                            preprocessed_title.append(unicode(a).encode("utf-8"))
                        except:
                            print traceback.format_exc()
                            print id
                            print a
                            continue

                    if debug: print "b-------title---------"
                    if debug: print title
                    if debug: print "a-------title---------"
                    if debug: print preprocessed_title

                    # process code of body part
                    if debug: print "b-codelist----------"
                    extracted_code_list = [a.strip() for a in re.findall("(?<=\<pre\>\<code\>).+?(?=\<\/code\>\</pre\>)", body,flags=re.DOTALL)]
                    if debug: print extracted_code_list
                    if debug: print "a-codelist----------"
                    original_code_word_list = []
                    for code in extracted_code_list:
                        parsed_code = self.html_parser.unescape(code)
                        for line in parsed_code .split("\n"):
                            for word in line.split():
                                try:
                                    original_code_word_list.append(unicode(word.strip()).encode("utf-8"))
                                except:
                                    print traceback.format_exc()
                                    print id
                                    print a
                                    continue

                    if debug: print original_code_word_list
                    preprocessed_code = []
                    for code in extracted_code_list:
                        parsed_code = self.html_parser.unescape(code)
                        for line in parsed_code.split("\n"):
                            for a in self.preprocess_line(line):
                                try:
                                    preprocessed_code.append(unicode(a).encode("utf-8"))
                                except:
                                    print traceback.format_exc()
                                    print id
                                    print a
                                    continue
                    if debug: print "preprocessed-codelist----------"
                    if debug: print preprocessed_code


                    # process natural language of body

                    body_without_code = re.sub(r"<pre><code>.+</code></pre>","",body,flags=re.DOTALL)

                    # process strong word of body
                    preprocessed_strong_list = []
                    # original strong word
                    original_strong_list = [a.strip() for a in re.findall(r"((?<=\<code\>).+?(?=\<\/code\>)|(?<=\<strong\>).+?(?=\<\/strong\>)|(?<=\<em\>).+?(?=\<\/em\>))",body_without_code,flags=re.DOTALL) if a not in ["Possible Duplicate:", "Possible Duplicates:"]]
                    for strong_word in original_strong_list:
                        parsed_strong_word = self.html_parser.unescape(strong_word)
                        for line in parsed_strong_word.split("\n"):
                            for a in self.preprocess_line(line):
                                try:
                                    preprocessed_strong_list.append(unicode(a).encode("utf-8"))
                                except:
                                    print traceback.format_exc()
                                    print id
                                    print a
                                    continue
                    original_strong_list = [unicode(a).encode("utf-8") for a in original_strong_list]
                    if debug: print "--extracted-strong-list -----"
                    if debug: print original_strong_list
                    if debug: print "--preprocessed-strong-list -----"
                    if debug: print preprocessed_strong_list

                    # preprocess body without code
                    # extract <p> part
                    sentences = [a.strip() for a in re.findall("(?<=\<p\>).+?(?=\<\/p\>)",
                                           body_without_code, flags=re.DOTALL)]
                    preprocessed_body = []
                    for sentence in sentences:
                        if "<strong>Possible Duplicate:</strong>" in sentence:
                            continue
                        if "<strong>Possible Duplicates:</strong>" in sentence:
                            continue
                        for line in sentence.split("\n"):
                            for a in self.preprocess_line(line):
                                if a != "":
                                    try:
                                        preprocessed_body.append(unicode(a).encode("utf-8"))
                                    except:
                                        print traceback.format_exc()
                                        print id
                                        print a
                                        continue

                    # go away <p> part from body
                    other = re.sub("\<p\>.+?\<\/p\>", "", body_without_code,flags=re.DOTALL)
                    for line in other.split("\n"):
                        for a in self.preprocess_line(line):
                            if a != "":
                                try:
                                    preprocessed_body.append(
                                        unicode(a).encode("utf-8"))
                                except:
                                    print traceback.format_exc()
                                    print id
                                    print a
                                    continue

                    if debug: print "------body---------"
                    if debug: print body
                    if debug: print "-------body_without_code---------"
                    if debug: print body_without_code
                    if debug: print "------sentences---------"
                    if debug: print sentences
                    if debug: print "b------other---------"
                    if debug: print other
                    if debug: print "a------other---------"
                    if debug: print preprocessed_other
                    if debug: print "------preprocessed_sentences---------"
                    if debug: print preprocessed_sentences
                    if debug: print "joined------preprocessed_body---------"
                    if debug: print " ".join(preprocessed_sentences)
                    preprocessed_list.append([id, " ".join([a.lower() for a in preprocessed_title]),
                                              " ".join([a.lower() for a in preprocessed_body]),
                                              " ".join([a.lower() for a in tag_list]),
                                              " ".join([a.lower() for a in preprocessed_strong_list]),
                                              " ".join([a.lower() for a in original_strong_list]),
                                              " ".join([a.lower() for a in preprocessed_code]),
                                              " ".join([a.lower() for a in original_code_word_list])])


                except:
                    print traceback.format_exc()
                    continue

        csv_file = open(self.work_dir + "preprocessed_" + filename.split("/")[-1] + ".csv", "wb")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(preprocessed_list)
        csv_file.close()
        print "end : " + "".join(filename)
        #if debug == False: os.remove(filename)
        #print datetime.datetime.now()


    def preprocess_line(self, line):
        if debug: print  "---before preprocess----"
        if debug: print line
        try:
            # print "---------------"
            # print "before"

            line = re.sub(
                r"</?(a|b|blockquote|code|del|dd|dl|dt|em|h1|h2|h3|i|img|kbd|li|ol|p|pre|s|sup|sub|strong|strike|ul|br|hr)([^>]+?)?>",
                "", line,flags=re.DOTALL)
            line = self.html_parser.unescape(line)
            # print line
            tokenized_body = nltk.word_tokenize(line)
            if debug: print tokenized_body
            striped_body = [a.strip(u"”“’‘✓…’—") for a in tokenized_body]
            body_without_stopwords = [a for a in striped_body if
                                      a.lower() not in self.stops]
            if debug: print body_without_stopwords
            stemmed_body = [self.stemmer.stem(a) for a in body_without_stopwords]
            if debug: print stemmed_body
            symbol_removed = [a for a in stemmed_body if not re.match(r"[!-/:-@[-`{-~]", a)]
            if debug: print symbol_removed
            if debug: print "-symbol_removed-"
            """
            sentence_list = []
            sentence = []
            for word in symbol_removed:
                if word == "?" or word == ".":
                    sentence_list.append(sentence)
                    sentence = []
                else:
                    try:
                        word = unicode(word).encode("utf-8")
                        sentence.append(word)
                    except:
                        pass
            else:
                sentence_list.append(sentence)
"""
            if debug: print  "---after preprocess----"
            #if debug: print "#####".join([" ".join(s) for s in sentence_list if s != []])
            return symbol_removed
        except:
            print traceback.format_exc()
            pass

if __name__== "__main__":
    try:
        input_dir ="../0.data/dump_data/split_posts/"
        output_dir = "result/"
        input_files = set(os.listdir(input_dir))
        p = Preprocessor(output_dir)
        if debug:
            for file in input_files:
                results = p.preprocess(input_dir+ file)
        pool = Pool()
        #output_files = set([file[13:-4] for file in os.listdir(output_dir)])
        #files = list(input_files - output_files)
        if debug == False:
            results = pool.map(p.preprocess,
                           [(input_dir+file) for file in input_files])
        #results = pool.map(p.preprocess, [(input_dir, "splited_post00"),(input_dir, "splited_post02")])

    except:
        print traceback.format_exc()
        pass

