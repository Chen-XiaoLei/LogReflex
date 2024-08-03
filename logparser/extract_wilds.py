import re


def content2List(content):
    StrList = []
    index = 0
    while (index < len(content)):
        word_now = content[index]
        if (content[index:index + 3] == "<*>"):
            StrList.append("<*>")
            index = index + 3
        elif (content[index:index + 2] == "<>"):
            StrList.append("<*>")
            index = index + 2
        elif (content[index:index + 2] == "<*"):
            StrList.append("<*>")
            index = index + 2
        elif (content[index:index + 2] == "*>"):
            StrList.append("<*>")
            index = index + 2
        else:
            StrList.append(word_now)
            index += 1
    return StrList


def split_content(content, no_delimiters):
    StrList = []
    index = 0
    tmp = ""
    while (index < len(content)):
        word_now = content[index]
        if (content[index:index + 3] == "<*>"):
            if (tmp):
                StrList.append(tmp)
                tmp = ""
            StrList.append("<*>")
            index += 3
        elif (word_now in no_delimiters or word_now.isalnum()):
            tmp += word_now
            index += 1
        else:
            if (tmp):
                StrList.append(tmp)
                tmp = ""
            StrList.append(word_now)
            index += 1
    if (tmp):
        StrList.append(tmp)
    return StrList


def lcs(strL1_o, strL2_o):
    strL1 = strL1_o.copy()
    strL2 = strL2_o.copy()
    strL1.reverse()
    strL2.reverse()
    len1 = len(strL1)
    len2 = len(strL2)

    dp = [[0 for column in range(len2 + 1)] for row in range(len1 + 1)]
    trace_back = [["None" for column in range(len2 + 1)] for row in range(len1 + 1)]

    for i in range(len(dp)):
        trace_back[i][0] = 'up'
    for i in range(len(dp[0])):
        trace_back[0][i] = 'left'
    trace_back[0][0] = 'start'

    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            if (strL1[i - 1] == strL2[j - 1]):
                dp[i][j] = dp[i - 1][j - 1] + 1
                trace_back[i][j] = 'diag'
            else:
                if (dp[i - 1][j] >= dp[i][j - 1]):
                    dp[i][j] = dp[i - 1][j]
                    trace_back[i][j] = 'up'
                else:
                    dp[i][j] = dp[i][j - 1]
                    trace_back[i][j] = 'left'
    pairs = []
    i_now = len1
    j_now = len2
    while (trace_back[i_now][j_now] != 'start'):
        if (trace_back[i_now][j_now] == 'diag'):
            pairs.append([i_now - 1, j_now - 1])
            i_now -= 1
            j_now -= 1
        elif (trace_back[i_now][j_now] == 'up'):
            i_now -= 1
        else:
            j_now -= 1
    new_pairs = []
    for pair in pairs:
        if (strL1[pair[0]] != strL2[pair[1]] or strL1[pair[0]] == "<*>"):
            continue
        new_pairs.append([len1 - 1 - pair[0], len2 - 1 - pair[1]])
    return new_pairs


def check_characters(content):
    character_types = set()
    for c in content:
        if (c.isdigit()):
            for i in "0123456789":
                character_types.add(i)
        elif (c.isalpha()):
            alphas = "qwertyuioplkjhgfdsazxcvbnmQWERTYUIOPLKJHGFDSAZXCVBNM"
            for i in alphas:
                character_types.add(i)
        else:
            character_types.add(c)
    return list(character_types)


def match_wildcard_with_content(template, log):
    # templateL = split_content("※" + template + "※", [])
    # logL = split_content("※" + log + "※", [])
    templateL = content2List("※ " + template + " ※")
    logL = content2List("※ " + log + " ※")
    alignPairL = lcs(logL, templateL)
    T_index = 0
    L_index = 0
    A_index = 0
    template_new = ""
    wildcards = []
    wild_content = []
    while (T_index < len(templateL) and L_index < len(logL) and A_index < len(alignPairL)):
        pair = alignPairL[A_index]
        if (T_index == pair[1] and L_index == pair[0]):
            template_new += templateL[T_index]
            T_index += 1
            L_index += 1
            A_index += 1
            continue

        if ('<*>' in templateL[T_index:pair[1]]):
            tmp = ""
            for i in range(L_index, pair[0]):
                tmp += logL[i]
            tmp, wildstr = process_space_in_wild(tmp)
            template_new += wildstr
            wild_content.append(tmp)
            wildcards.append(check_characters(tmp))
        else:
            for i in range(L_index, pair[0]):
                template_new += logL[i]
        T_index = pair[1]
        L_index = pair[0]

    template_new = template_new.replace("※", "").strip()
    return template_new, wildcards, wild_content


def process_space_in_wild(tmp):
    wildstr = "<*>"
    if (tmp and tmp[0] == " "):
        tmp = tmp[1:]
        wildstr = " " + wildstr
    if (tmp and tmp[-1] == " "):
        tmp = tmp[:-1]
        wildstr = wildstr + " "
    return tmp, wildstr

def process_(template, log):
    templateL=content2List(template)
    template_L=[]
    tmp=""
    for temp in templateL:
        if(temp.isalnum() or temp=="<*>"):
            tmp+=temp
        else:
            if(tmp):
                template_L.append(tmp)
                tmp=""
            template_L.append(temp)
    if (tmp):
        template_L.append(tmp)
    template_new=""
    for ch in template_L:
        if("<*>" in ch):
            template_new+="<*>"
        else:
            template_new+=ch
    template, wildcards, wild_content = match_wildcard_with_content(template_new, log)
    return template, wildcards, wild_content

def preprocess_before_insert_into_index(template, wildcards, wild_content):

    template_new = ""
    wildcards_new = []
    wild_content_new = []
    index = 0
    wild_index = 0
    while (index < len(template)):
        word_now = template[index]
        if (template[index:index + 3] == "<*>"):
            wild_now = wildcards[wild_index]
            wild_content_now = wild_content[wild_index]
            next_word = ""
            if (index + 3 < len(template) and not template[index + 3].isalnum()):
                next_word = template[index + 3]
            if (next_word and next_word in wild_now):
                wild_list = wild_content_now.split(next_word)
                for w in wild_list:
                    template_new += "<*>" + next_word
                    wildcards_new.append(check_characters(w))
                    wild_content_new.append(w)
                template_new = template_new[:-1]
            else:
                if(wild_now):
                    template_new += "<*>"
                    wildcards_new.append(wild_now)
                    wild_content_new.append(wild_content_now)
            wild_index += 1
            index = index + 3
        else:
            template_new += word_now
            index += 1
    return template_new.strip(), wildcards_new, wild_content_new


def merge_candidate(list):
    count = 0
    for unit in list:
        if (unit['content'] != ""):
            count += 1
    return count


def contain_alnum(content):
    for ch in content:
        if (ch.isalnum()):
            return True
    return False


def get_candidates(template, log):
    log_tmp = log
    log_tmp = re.sub(r'/[a-zA-Z0-9\-._~:/?#@!$&\'*+,;=]*', '<*>', log_tmp)
    log_tmp = re.sub(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b', '<*>', log_tmp)
    template_tmp = re.sub("<\*>", "☆", template)
    a, _, wild_content1 = match_wildcard_with_content(log_tmp, template_tmp)
    _, _, wild_content2 = match_wildcard_with_content(log_tmp, log)
    candidate_log_list = []
    candidate_template_list = []
    for i in range(len(wild_content1)):
        content_t = wild_content1[i]
        content_l = wild_content2[i]
        if (not contain_alnum(content_t)):
            continue
        candidate_log_list.append(content_l)
        candidate_template_list.append(content_t.replace("☆", "<*>"))
    template_candidate = template
    for content in candidate_template_list:
        template_candidate = template_candidate.replace(content, "<*>")

    template_candidate, _, wild_content = match_wildcard_with_content(template_candidate, log)

    templateL = content2List(template_candidate)
    wild_index = 0
    tmp = ""
    candidate_units = []
    for ch in templateL:
        if (ch != "<*>"):
            tmp += ch
        else:
            if (tmp):
                candidate_units.append({"template": tmp, "content": ""})
                tmp = ""
            if (wild_content[wild_index].isalpha() or wild_content[wild_index] in candidate_log_list):
                candidate_units.append({"template": "<*>", "content": wild_content[wild_index]})
            else:
                candidate_units.append({"template": "<*>", "content": ""})
            wild_index += 1
    if (tmp):
        candidate_units.append({"template": tmp, "content": ""})

    candidate_units_final = []
    for unit in candidate_units:
        if (unit["content"] != ""):
            candidate_units_final.append(unit)
        else:
            contentL = split_content(unit["template"], [])
            tmp = ""
            unit_List_tmp = []
            for c in contentL:
                if (not c.isdigit()):
                    tmp += c
                else:
                    if (tmp):
                        unit_List_tmp.append({"template": tmp, "content": ""})
                        tmp = ""
                    unit_List_tmp.append({"template": "<*>", "content": c})
            if (tmp):
                unit_List_tmp.append({"template": tmp, "content": ""})
            unit_List_tmp_new = []
            tmp_list = []
            for unit in unit_List_tmp:
                if (unit["content"] == ""):
                    if (not tmp_list):
                        unit_List_tmp_new.append(unit)
                    else:
                        if (contain_alnum(unit["template"])):
                            count = merge_candidate(tmp_list)
                            index = 0
                            unit_new = {"template": "<*>", "content": ""}
                            for u in tmp_list:
                                if (index < count):
                                    if (u["template"] == "<*>"):
                                        unit_new['content'] += u['content']
                                        index += 1
                                    else:
                                        unit_new['content'] += u['template']

                                else:
                                    if (index == count):
                                        unit_List_tmp_new.append(unit_new)
                                        index += 1
                                    unit_List_tmp_new.append(u)
                            if (index == count):
                                unit_List_tmp_new.append(unit_new)
                                index += 1
                            unit_List_tmp_new.append(unit)
                            tmp_list = []
                        else:
                            tmp_list.append(unit)
                else:
                    tmp_list.append(unit)

            if (tmp_list):
                count = merge_candidate(tmp_list)
                index = 0
                unit_new = {"template": "<*>", "content": ""}
                for u in tmp_list:
                    if (index < count):
                        if (u["template"] == "<*>"):
                            unit_new['content'] += u['content']
                            index += 1
                        else:
                            unit_new['content'] += u['template']

                    else:
                        if (index == count):
                            unit_List_tmp_new.append(unit_new)
                            index += 1
                        unit_List_tmp_new.append(u)
                if (index == count):
                    unit_List_tmp_new.append(unit_new)
                    index += 1
            for unit in unit_List_tmp_new:
                candidate_units_final.append(unit)

    return candidate_units_final


def merge_two_template(template1, template2):
    template1L = split_content("※ " + template1 + " ※", [])
    template2L = split_content("※ " + template2 + " ※", [])
    alignPairL = lcs(template1L, template2L)
    T1_index = 0
    T2_index = 0
    A_index = 0
    wilds = []
    template = ""

    while (T1_index < len(template1L) and T2_index < len(template2L) and A_index < len(alignPairL)):
        pair = alignPairL[A_index]
        if (T1_index == pair[0] and T2_index == pair[1]):
            template += template1L[T1_index]
            T1_index += 1
            T2_index += 1
            A_index += 1
        else:
            template += "<*>"
            tmp1 = ""
            for i in range(T1_index, pair[0]):
                tmp1 += template1L[i]
            tmp2 = ""
            for i in range(T2_index, pair[1]):
                tmp2 += template2L[i]
            wilds.append([tmp1, tmp2])
            T1_index = pair[0]
            T2_index = pair[1]
    template = template.replace("※", "").strip()
    return template, wilds


def delete_common(str1, str2):
    prefix = []
    for c1, c2 in zip(str1, str2):
        if (c1 == c2 and c1 != "<"):
            prefix.append(c1)
        else:
            break
    common_prefix = ''.join(prefix)

    suffix = []
    for c1, c2 in zip(str1[::-1], str2[::-1]):
        if (c1 == c2 and c1 != ">"):
            suffix.append(c1)
        else:
            break
    common_suffix = ''.join(suffix[::-1])
    l1 = len(common_prefix)
    l2 = len(common_suffix)
    if (l1 == 0 and l2 == 0):
        str1 = str1
        str2 = str2
    elif (l2 == 0):
        str1 = str1[l1:]
        str2 = str2[l1:]
    else:
        str1 = str1[l1:l2*-1]
        str2 = str2[l1:l2*-1]
    return str1, str2

def merge_horizontally(template):
    templateL=template.split()
    new_template=""
    last=[]
    for str in templateL:
        if(str in last and "<*>" in last[0]):
            if("<*>" not in last):
                space_index=new_template.strip().rfind(" ")
                new_template=new_template[:space_index]
                new_template +=" <*> "
                last.append("<*>")
            continue
        new_template+=str+" "
        last=[str]
    template=new_template.strip()
    return template

def merge_wilds(template):
    template_new = ""
    templateL = content2List(template)
    ids = []
    for id in range(len(templateL)):
        ch = templateL[id]
        if (ch == "<*>"):
            ids.append(id)

    for i in range(len(ids) - 1):
        id1 = ids[i]
        id2 = ids[i + 1]
        have_alnum = False
        for j in range(id1, id2):
            if (contain_alnum(templateL[j]) or templateL[j] == " "):
                have_alnum = True
                break
        if (not have_alnum):
            for j in range(id1, id2):
                templateL[j] = ""
    for ch in templateL:
        if (ch == "<*>"):
            if (len(template_new) > 3 and template_new[-3:] == "<*>"):
                continue
        template_new += ch
    return template_new
