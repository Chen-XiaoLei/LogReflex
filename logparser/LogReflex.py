import pandas as pd
from logparser.dataloader import load_data, candidate_set_construction
from logparser.retrieval import index_tree
from logparser.extract_wilds import match_wildcard_with_content, preprocess_before_insert_into_index, get_candidates, \
    merge_two_template, merge_wilds, delete_common, merge_horizontally, process_
from logparser.KNN import invert_index, template_invert_index
from logparser.prompt import EXTRACT_TEMPLATE, VARIABLE_CONSTANT, MERGE_TEMPLATES
import os
import json
from logparser.LLM import create_open_llm, OpenLLMAPI
from langchain import PromptTemplate, LLMChain
import re


class LogCluster:
    def __init__(self, logIDL=[], template=""):
        self.template = template
        self.logIDL = logIDL
        self.logs = []


def get_template(template):
    if ("<END>" not in template):
        template += "<END>"
    if (len(re.findall("<START>", template)) == 1 and len(re.findall("<END>", template)) == 1):
        pattern = re.compile('<START>(.+)<END>')
        result = pattern.findall(template)
        result = result[0]
    else:
        parts = re.split("<START>", template)
        result = ""
        for p in parts:
            if ("<END>" not in p):
                continue
            else:
                result = p.replace("<END>", "")
    return result


class LogParser:
    def __init__(self, dataset, cache_path, indir, outdir, k=3, candidate_num=32, Groups=[]):
        self.path = indir
        self.dataset = dataset
        self.savePath = outdir
        self.logClusters = []
        self.df_log = load_data(indir, dataset)
        self.cache_path = cache_path
        self.load_cache()
        self.k = k
        self.candidate_num = candidate_num
        self.Groups=Groups
        self.load_components()

    def load_cache(self):
        if (not os.path.exists("{}/cache_extract/{}.json".format(self.cache_path, self.dataset))):
            if (not os.path.exists("{}/cache_extract".format(self.cache_path))):
                os.makedirs("{}/cache_extract".format(self.cache_path))
            with open("{}/cache_extract/{}.json".format(self.cache_path, self.dataset), "w+") as f:
                json.dump({}, f)
        if (not os.path.exists("{}/cache_variable_constant/{}.json".format(self.cache_path, self.dataset))):
            if (not os.path.exists("{}/cache_variable_constant".format(self.cache_path))):
                os.makedirs("{}/cache_variable_constant".format(self.cache_path))
            with open("{}/cache_variable_constant/{}.json".format(self.cache_path, self.dataset), "w+") as f:
                json.dump({}, f)
        if (not os.path.exists("{}/cache_merge/{}.json".format(self.cache_path, self.dataset))):
            if (not os.path.exists("{}/cache_merge".format(self.cache_path))):
                os.makedirs("{}/cache_merge".format(self.cache_path))
            with open("{}/cache_merge/{}.json".format(self.cache_path, self.dataset), "w+") as f:
                json.dump({}, f)

        with open("{}/cache_extract/{}.json".format(self.cache_path, self.dataset), "r+") as f:
            self.cache_extract = json.load(f)
        with open("{}/cache_variable_constant/{}.json".format(self.cache_path, self.dataset), "r+") as f:
            self.cache_variable_constant = json.load(f)
        with open("{}/cache_merge/{}.json".format(self.cache_path, self.dataset), "r+") as f:
            self.cache_merge = json.load(f)

    def test_LLM(self):
        try:
            response=self.llm.predict("Hello")
        except:
            print("LLM disabled")

    def load_components(self):
        try:
            self.llm = create_open_llm("http://xxxxxxx")
        except:
            self.llm = None
            print("LLM not Connected, Parsing Logs based on the Cache data")
        self.test_LLM()
        self.CI_tree = index_tree()
        self.Invert_Index = template_invert_index()

        candidate_set = candidate_set_construction(self.path, self.dataset, self.candidate_num, self.Groups)
        print("There are {} samples in RS.".format(len(candidate_set)))
        self.KNN = invert_index(candidate_set)

        if(self.llm is not None):
            prompt_extract = PromptTemplate(template=EXTRACT_TEMPLATE, input_variables=["log", "examples"])
            self.llm_extract = LLMChain(prompt=prompt_extract, llm=self.llm)

            prompt_v_c = PromptTemplate(template=VARIABLE_CONSTANT, input_variables=["log", "snippet"])
            self.llm_v_c = LLMChain(prompt=prompt_v_c, llm=self.llm)

            prompt_merge = PromptTemplate(template=MERGE_TEMPLATES, input_variables=["template1", "template2"])
            self.llm_merge = LLMChain(prompt=prompt_merge, llm=self.llm)

    def outputResults(self):
        filename = self.dataset
        df_event = []
        ids = [-1] * self.df_log.shape[0]
        templates = [""] * self.df_log.shape[0]

        for cid in range(len(self.logClusters)):
            cluster = self.logClusters[cid]
            df_event.append([cid, cluster.template, len(cluster.logIDL)])

            for id in cluster.logIDL:
                ids[id] = cid
                templates[id] = cluster.template

        df_event = pd.DataFrame(df_event, columns=['EventId', 'EventTemplate', 'Occurrences'])

        with open("{}/cache_extract/{}.json".format(self.cache_path, self.dataset), "w+") as f:
            json.dump(self.cache_extract, f, indent=4)
        with open("{}/cache_variable_constant/{}.json".format(self.cache_path, self.dataset), "w+") as f:
            json.dump(self.cache_variable_constant, f, indent=4)
        with open("{}/cache_merge/{}.json".format(self.cache_path, self.dataset), "w+") as f:
            json.dump(self.cache_merge, f, indent=4)

        self.df_log['EventId'] = ids
        self.df_log['EventTemplate'] = templates
        self.df_log.to_csv(os.path.join(self.savePath, filename + '_2k.log_structured.csv'), index=False,
                           encoding="utf-8")
        df_event.to_csv(os.path.join(self.savePath, filename + '_2k.log_templates.csv'), index=False, encoding="utf-8")

    def initial_parse(self, log):
        if (log in self.cache_extract):
            return self.cache_extract[log]
        examples = self.KNN.query(log, k=self.k)
        example_prompt = ""
        for example in examples:
            example_prompt += "Log:\n<START>{}<END>\nTemplate:\n<START>{}<END>\n\n".format(example['log'],
                                                                                           example['template'])
        parsed_log = self.llm_extract.run(examples=example_prompt, log=log)
        initial_template = get_template(parsed_log)
        self.cache_extract[log] = initial_template
        return initial_template

    def parsing_refinement(self, template, log):
        candidates = get_candidates(template, log)
        template_new = ""
        for candidate in candidates:
            if (candidate["content"] == ""):
                template_new += candidate["template"]
            else:
                if (self.v_c_call(log, candidate["content"])):
                    template_new += "<*>"
                else:
                    template_new += candidate["content"]
        return template_new

    def parse_result(self, response):
        response = response.lower()
        if ('yes' in response):
            return True
        else:
            return False

    def v_c_call(self, log, content):
        if (log in self.cache_variable_constant.keys()):
            if (content in self.cache_variable_constant[log].keys()):
                response = self.cache_variable_constant[log][content]
                return self.parse_result(response)
        response = self.llm_v_c.run(log=log, snippet=content)

        if (log not in self.cache_variable_constant.keys()):
            self.cache_variable_constant[log] = {}
        self.cache_variable_constant[log][content] = response
        return self.parse_result(response)

    def cover(self, template1, template2):
        template_merge, wilds = merge_two_template(template1, template2)
        if (len(re.findall('[a-zA-Z0-9]', template_merge)) == 0):
            return False, template_merge, wilds
        if (template1 == template2):
            return True, template_merge, wilds
        for w1, w2 in wilds:
            w1, w2 = delete_common(w1, w2)
            if (w1 != "<*>" and w2 != "<*>"):
                if ((w1 == "" and "<*>" in w2 and len(re.findall("[a-zA-Z0-9]", w2)) == 0) or (
                        w2 == "" and "<*>" in w2 and len(re.findall("[a-zA-Z0-9]", w1)) == 0)):
                    continue
                return False, template_merge, wilds
        return True, template_merge, wilds

    def more_element(self, template1, template2):
        template1_new = merge_horizontally(template1)
        template2_new = merge_horizontally(template2)
        if (template1_new == template1 and template2_new == template2):
            return None
        cover, template_merge, wilds = self.cover(template1_new, template2_new)
        if (cover):
            return template_merge
        else:
            return None

    def template_refinement(self, template):
        for cid in range(len(self.logClusters)):
            if (self.logClusters[cid].template == template):
                return cid, template

        index_result = self.Invert_Index.query(template, 1)
        if (index_result):
            most_similar_cid, score = index_result[0]
        else:
            return -1, ""
        template_most_similar = self.logClusters[most_similar_cid].template

        template_merge = self.more_element(template, template_most_similar)
        if (template_merge is not None):
            return most_similar_cid, template_merge

        cover, template_merge, wilds = self.cover(template, template_most_similar)
        if (cover):
            return most_similar_cid, template_merge

        if (self.merge_call(template_most_similar, template)):
            return most_similar_cid, template_merge

        return -1, ""

    def merge_call(self, template1, template2):
        if (template1 in self.cache_merge.keys()):
            if (template2 in self.cache_merge[template1].keys()):
                response = self.cache_merge[template1][template2]
                return self.parse_result(response)
        if (template2 in self.cache_merge.keys()):
            if (template1 in self.cache_merge[template2].keys()):
                response = self.cache_merge[template2][template1]
                return self.parse_result(response)

        response = self.llm_merge.run(template1=template1, template2=template2)
        if (template1 not in self.cache_merge.keys()):
            self.cache_merge[template1] = {}
        self.cache_merge[template1][template2] = response
        return self.parse_result(response)

    def merge_same_clusters(self, cid):
        cidL = [cid]
        log_idL = self.logClusters[cid].logIDL.copy()
        logs = self.logClusters[cid].logs.copy()
        for id in range(len(self.logClusters)):
            if (cid == id):
                continue
            template1 = self.logClusters[cid].template
            template2 = self.logClusters[id].template
            merge, template_merge, new_Wilds = self.cover(template1, template2)
            if (not merge):
                continue
            if (template_merge == template1 or template_merge == template2):
                cidL.append(id)
                log_idL += self.logClusters[id].logIDL
                logs += self.logClusters[id].logs
                self.logClusters[cid].template = template_merge

        if (len(cidL) == 1):
            return

        new_Clusters = []
        new_prefix_tree = index_tree()
        new_index = template_invert_index()
        for id in range(len(self.logClusters)):
            if (id not in cidL):
                cid_now = len(new_Clusters)
                new_Clusters.append(self.logClusters[id])
                new_index.insert_template(self.logClusters[id].template, cid_now)
                for log in self.logClusters[id].logs:
                    template, wildcards, wild_content = match_wildcard_with_content(self.logClusters[id].template, log)
                    template, wildcards, wild_content = preprocess_before_insert_into_index(template, wildcards,
                                                                                            wild_content)
                    new_prefix_tree.insert_template(template, wildcards, cid_now)

        newCluster = LogCluster(log_idL, self.logClusters[cid].template)
        newCluster.logs = logs
        cid_now = len(new_Clusters)
        new_Clusters.append(newCluster)
        new_index.insert_template(newCluster.template, cid_now)
        for log in newCluster.logs:
            template, wildcards, wild_content = match_wildcard_with_content(newCluster.template, log)
            template, wildcards, wild_content = preprocess_before_insert_into_index(template, wildcards, wild_content)
            new_prefix_tree.insert_template(template, wildcards, cid_now)

        self.logClusters = new_Clusters
        self.CI_tree = new_prefix_tree
        self.Invert_Index = new_index

    def parse(self):
        print("=============== Parsing logs on dataset {} =================.".format(self.dataset))
        for idx, line in self.df_log.iterrows():
            if idx % 100 == 0:
                print('Processed {0:.1f}% of log lines.'.format(idx * 100.0 / len(self.df_log)))

            lineID = line['LineId']
            logmessage = line['Content'].strip()
            logid = lineID - 1
            logmessage = re.sub("  ", " ", logmessage)

            match_id = self.CI_tree.retrieval_template(logmessage)

            if (match_id != -1):
                self.logClusters[match_id].logIDL.append(logid)
                continue

            initial_template = self.initial_parse(logmessage)
            initial_template = merge_wilds(initial_template)

            template, wildcards, wild_content = match_wildcard_with_content(initial_template, logmessage)
            template = self.parsing_refinement(template, logmessage)
            template = merge_wilds(template)

            most_similar_cid, template_merge = self.template_refinement(template)
            template_merge = merge_wilds(template_merge)

            if (most_similar_cid != -1):
                self.logClusters[most_similar_cid].logIDL.append(logid)
                self.logClusters[most_similar_cid].logs.append(logmessage)
                self.logClusters[most_similar_cid].template = template_merge

                template, wildcards, wild_content = match_wildcard_with_content(template_merge, logmessage)
                template, wildcards, wild_content = process_(template, logmessage)
                template, wildcards, wild_content = preprocess_before_insert_into_index(template, wildcards,
                                                                                        wild_content)
                self.CI_tree.insert_template(template, wildcards, most_similar_cid)
                self.merge_same_clusters(most_similar_cid)
                continue

            cid = len(self.logClusters)
            template, wildcards, wild_content = match_wildcard_with_content(template, logmessage)
            newCluster = LogCluster([logid], template)
            newCluster.logs.append(logmessage)
            self.logClusters.append(newCluster)
            template, wildcards, wild_content = preprocess_before_insert_into_index(template, wildcards, wild_content)

            self.CI_tree.insert_template(template, wildcards, cid)
            self.Invert_Index.insert_template(template, cid)
            self.merge_same_clusters(cid)

        self.outputResults()
