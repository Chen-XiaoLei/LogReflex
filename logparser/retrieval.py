class tree_node:
    def __init__(self, type, content, stop_ch, wilds, cid):
        self.type = type
        self.content = content
        self.wilds = wilds
        self.stop_ch = stop_ch
        self.next = {}
        self.cid = cid

    def match(self, log):
        if (self.type == "wild"):
            for i in range(len(log)):
                ch = log[i]
                # if (ch not in self.wilds or ch in self.stop_ch):
                if (ch not in self.wilds):
                    return -1, True, log[i:]
            return -1, True, ""
        elif (self.type == "constant"):
            if (log[:len(self.content)] == self.content):
                return -1, True, log[len(self.content):]
            else:
                return -1, False, ""
        elif (self.type == "template"):
            if (log != ""):
                return -1, False, ""
            else:
                return self.cid, True, ""
        else:
            return -1, True, log


def segment_template(template, wilds):
    Unit_List = []
    index = 0
    wild_index = 0
    while (index < len(template)):
        word_now = template[index]
        if (template[index:index + 3] == "<*>"):
            Unit_List.append(segment_unit("<*>", wilds[wild_index]))
            wild_index += 1
            index = index + 3
        else:
            Unit_List.append(segment_unit(word_now, None))
            index += 1
    return Unit_List


class segment_unit:
    def __init__(self, content, wilds):
        self.content = content
        self.wilds = wilds


class index_tree:
    def __init__(self):
        self.root = tree_node(type="root", content="", stop_ch=[], wilds=[], cid=-1)

    def retrieval_template(self, log, node_=None):
        if (node_ is None):
            node_now = self.root
        else:
            node_now = node_
        cid, not_fail, log = node_now.match(log)
        if (cid != -1):
            return cid
        if (not not_fail):
            return -1
        if ("template" in node_now.next.keys()):
            cid = self.retrieval_template(log, node_now.next["template"])
            if (cid != -1):
                return cid
        if (log == ""):
            return -1
        if (log[0] in node_now.next.keys()):
            cid = self.retrieval_template(log, node_now.next[log[0]])
            if (cid != -1):
                return cid
        if ("<*>" in node_now.next.keys()):
            cid = self.retrieval_template(log, node_now.next["<*>"])
            if (cid != -1):
                return cid
        return -1

    def insert_template(self, template, wilds, cid):
        node_now = self.root
        Unit_List = segment_template(template, wilds)
        Unit_index = 0
        while (Unit_index < len(Unit_List)):
            unit = Unit_List[Unit_index]
            if (unit.content in node_now.next.keys()):
                last_node = node_now
                node_now = node_now.next[unit.content]
                if (node_now.type == "wild"):
                    stop_word = ""
                    if (Unit_index + 1 < len(Unit_List)):
                        stop_word = Unit_List[Unit_index + 1].content
                    if (stop_word and stop_word in node_now.wilds):
                        wild_ = node_now.wilds.copy()
                        wild_.remove(stop_word)
                        new_node = tree_node(type="wild", content="<*>", stop_ch=stop_word, wilds=wild_, cid=-1)
                        node_now.wilds.remove(stop_word)
                        new_node2 = tree_node(type="constant", content=stop_word, stop_ch=[], wilds=[], cid=-1)
                        new_node.next[stop_word] = new_node2
                        new_node2.next["<*>"] = node_now
                        node_now = new_node
                        last_node.next["<*>"] = node_now
                    p=False
                    for symbol in unit.wilds:
                        if(symbol.isalnum()):
                            continue
                        if(symbol in node_now.next.keys()):
                            p=True
                            while(1):
                                node_now.wilds = node_now.wilds + [item for item in unit.wilds if
                                                                   item not in (node_now.wilds + [symbol])]
                                if(symbol in node_now.next.keys() and "<*>" in node_now.next[symbol].next.keys() and node_now.next[symbol].content==symbol):
                                    node_now=node_now.next[symbol].next["<*>"]
                                else:
                                    break
                            break
                    if(not p):
                        node_now.wilds = node_now.wilds + [item for item in unit.wilds if item not in node_now.wilds]
                    Unit_index += 1
                elif (node_now.type == "constant"):
                    content_to_match = node_now.content
                    ch_index = 0
                    while (ch_index < len(content_to_match)):
                        if (Unit_index >= len(Unit_List)):
                            node_now.content = content_to_match[:ch_index]
                            new_node = tree_node(type="constant", content=content_to_match[ch_index:], stop_ch=[],
                                                 wilds=[], cid=-1)
                            new_node.next = node_now.next
                            node_now.next = {content_to_match[ch_index:][0]: new_node}
                            new_node = tree_node(type="template", content=template, stop_ch=[], wilds=[], cid=cid)
                            node_now.next['template'] = new_node
                            return

                        ch1 = content_to_match[ch_index]
                        ch2 = Unit_List[Unit_index].content
                        if (ch1 == ch2):
                            ch_index += 1
                            Unit_index += 1
                        else:
                            node_now.content = content_to_match[:ch_index]
                            new_node = tree_node(type="constant", content=content_to_match[ch_index:], stop_ch=[],
                                                 wilds=[], cid=-1)
                            new_node.next = node_now.next
                            node_now.next = {content_to_match[ch_index:][0]: new_node}
                            break
            else:
                tmp = ""
                for i in range(Unit_index, len(Unit_List)):
                    unit = Unit_List[i]
                    if (unit.content != "<*>"):
                        tmp += unit.content
                    else:
                        if (tmp):
                            new_node = tree_node(type="constant", content=tmp, stop_ch=[], wilds=[], cid=-1)
                            node_now.next[tmp[0]] = new_node
                            tmp = ""
                            node_now = new_node

                        stop_ch = []
                        if (i + 1 < len(Unit_List)):
                            stop_ch.append(Unit_List[i + 1].content)
                        new_node = tree_node(type="wild", content="<*>", stop_ch=stop_ch, wilds=unit.wilds, cid=-1)
                        node_now.next["<*>"] = new_node
                        node_now = new_node
                if (tmp):
                    new_node = tree_node(type="constant", content=tmp, stop_ch=[], wilds=[], cid=-1)
                    node_now.next[tmp[0]] = new_node
                    node_now = new_node

                new_node = tree_node(type="template", content=template, stop_ch=[], wilds=[], cid=cid)
                node_now.next['template'] = new_node
                return
        new_node = tree_node(type="template", content=template, stop_ch=[], wilds=[], cid=cid)
        node_now.next['template'] = new_node
        return