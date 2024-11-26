import pandas as pd
import numpy as np
import json
from logparser.extract_wilds import match_wildcard_with_content, split_content


def map_func(x, y):
    if (x > y):
        z = (y * 1.0) / (x * 1.0) + 1.0
    else:
        z = (x * 1.0) / (y * 1.0) + 1.0
    return z
    # score = np.log(1 + 100 * z) + 1
    # return float(score)


def contains_letter(s):
    for char in s:
        if char.isalpha():
            return True
    return False


def split_all(content):
    tmp = ""
    contentL = []
    for ch in content:
        if (ch.isalnum()):
            tmp += ch
        else:
            if (tmp and contains_letter(tmp)):
                contentL.append(tmp)
                tmp = ""
    if (tmp and contains_letter(tmp)):
        contentL.append(tmp)
    return contentL


class invert_index_unit:
    def __init__(self, ids):
        self.ids = ids


class candidate:
    def __init__(self, log, template):
        self.log = log
        self.template = template


class invert_index:
    def __init__(self, data_candidate):
        self.table = {}
        self.load_candidates(data_candidate)
        constants = {}
        for key in self.candidates.keys():
            line = self.candidates[key]
            content = line.log
            constantL = split_all(content)

            for c in constantL:
                if (c not in constants.keys()):
                    constants[c] = []
                if (key not in constants[c]):
                    constants[c].append(key)


        key_set = set(constants.keys())
        for key in key_set:
            templates = []
            if (key in constants.keys()):
                templates += constants[key]

            unit = invert_index_unit(templates)
            self.table[key] = unit


    def load_candidates(self, data_candidate):
        self.candidates = {}
        for line in data_candidate:
            id = len(self.candidates.keys())
            self.candidates[id] = candidate(line['content'], line['template'])

    def query(self, log, k=3):
        logL = split_all(log)
        result_list = {}
        for id in self.candidates.keys():
            result_list[id] = 0.0
        self_score = 0.0
        for log_snippt in logL:
            if (log_snippt in self.table.keys()):
                unit = self.table[log_snippt]
                self_score += 1.0
                for id in unit.ids:
                    result_list[id] += 1.0
            else:
                self_score += 1.0
        for key in result_list.keys():
            score_now = result_list[key]
            result_list[key] = score_now / (self_score + self.candidates[key].score - score_now)

        sorted_list = sorted(result_list.items(), key=lambda item: item[1], reverse=True)[:k]
        sorted_dict = dict(sorted_list)

        result = []
        for key in sorted_dict.keys():
            result.append(
                {"score": result_list[key], "log": self.candidates[key].log,
                 "template": self.candidates[key].template})
        result.reverse()
        return result


class template_invert_index:
    def __init__(self):
        self.word_table = {}
        self.id_table = {}

    def insert_template(self, template, tid):
        templateL = split_content(template, ['-', '_', '/', '.'])
        count = 0
        for token in templateL:
            if (token.isalpha()):
                count += 1
                if (token not in self.word_table.keys()):
                    self.word_table[token] = {}
                if (tid not in self.word_table[token].keys()):
                    self.word_table[token][tid] = 0
                self.word_table[token][tid] += 1
        self.id_table[tid] = count

    def query(self, template, k):
        templateL = split_content(template, ['-', '_', '/', '.'])
        result = {}
        count=0
        for token in templateL:
            if (token.isalpha()):
                count+=1
                if (token in self.word_table.keys()):
                    for tid in self.word_table[token]:
                        if (tid not in result.keys()):
                            result[tid] = {'value': 0, 'contents': {}}
                        if (token not in result[tid]['contents'].keys()):
                            result[tid]['contents'][token] = 0
                        if (result[tid]['contents'][token] < self.word_table[token][tid]):
                            result[tid]['value'] += 1
                            result[tid]['contents'][token] += 1

        ret = {}
        for tid in result.keys():
            ret[tid] = (result[tid]['value'] * 1.0) / ((self.id_table[tid]+count) * 1.0)
        ret = sorted(ret.items(), key=lambda x: x[1], reverse=True)
        return ret[:k]
