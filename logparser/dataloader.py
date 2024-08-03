import re
import pandas as pd
import random
import Levenshtein
benchmark_settings = {
    'HDFS': {
        'log_file': 'HDFS/HDFS_2k.log',
        'log_format': '<Date> <Time> <Pid> <Level> <Component>: <Content>',
    },

    'Hadoop': {
        'log_file': 'Hadoop/Hadoop_2k.log',
        'log_format': '<Date> <Time> <Level> \[<Process>\] <Component>: <Content>',
    },

    'Spark': {
        'log_file': 'Spark/Spark_2k.log',
        'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    },

    'Zookeeper': {
        'log_file': 'Zookeeper/Zookeeper_2k.log',
        'log_format': '<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>',
    },
    'OpenStack': {
        'log_file': 'OpenStack/OpenStack_2k.log',
        'log_format': '<Logrecord> <Date> <Time> <Pid> <Level> <Component> \[<ADDR>\] <Content>',
    },

    'BGL': {
        'log_file': 'BGL/BGL_2k.log',
        'log_format': '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>',
    },

    'HPC': {
        'log_file': 'HPC/HPC_2k.log',
        'log_format': '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>',
    },

    'Thunderbird': {
        'log_file': 'Thunderbird/Thunderbird_2k.log',
        'log_format': '<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>',
    },

    'Windows': {
        'log_file': 'Windows/Windows_2k.log',
        'log_format': '<Date> <Time>, <Level>                  <Component>    <Content>',
    },

    'Linux': {
        'log_file': 'Linux/Linux_2k.log',
        'log_format': '<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>',
    },

    'Mac': {
        'log_file': 'Mac/Mac_2k.log',
        'log_format': '<Month>  <Date> <Time> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>',
    },

    'Android': {
        'log_file': 'Android/Android_2k.log',
        'log_format': '<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>',
    },

    'HealthApp': {
        'log_file': 'HealthApp/HealthApp_2k.log',
        'log_format': '<Time>\|<Component>\|<Pid>\|<Content>',
    },

    'Apache': {
        'log_file': 'Apache/Apache_2k.log',
        'log_format': '\[<Time>\] \[<Level>\] <Content>',
    },

    'Proxifier': {
        'log_file': 'Proxifier/Proxifier_2k.log',
        'log_format': '\[<Time>\] <Program> - <Content>',
    },

    'OpenSSH': {
        'log_file': 'OpenSSH/OpenSSH_2k.log',
        'log_format': '<Date> <Day> <Time> <Component> sshd\[<Pid>\]: <Content>',
    },
}

datasets = [
    'HDFS', 'Hadoop', 'Spark', 'Zookeeper', 'OpenStack', 'BGL', 'HPC', 'Thunderbird', 'Windows', 'Linux', 'Mac',
    'Android', 'HealthApp', 'Apache', 'Proxifier', 'OpenSSH']


def generate_logformat_regex(logformat):
    headers = []
    splitters = re.split(r'(<[^<>]+>)', logformat)
    regex = ''
    for k in range(len(splitters)):
        if k % 2 == 0:
            splitter = re.sub(' +', '\\\s+', splitters[k])
            regex += splitter
        else:
            header = splitters[k].strip('<').strip('>')
            regex += '(?P<%s>.*?)' % header
            headers.append(header)
    regex = re.compile('^' + regex + '$')
    return headers, regex

def log_to_dataframe(log_file, regex, headers):
    log_messages = []
    linecount = 0
    with open(log_file, 'r', encoding='utf-8') as fin:
        lines = fin.readlines()
        for line in lines:
            try:
                match = regex.search(line.strip())
                message = [match.group(header) for header in headers]
                log_messages.append(message)
                linecount += 1
            except Exception as e:
                pass
    logdf = pd.DataFrame(log_messages, columns=headers)
    logdf.insert(0, 'LineId', None)
    logdf['LineId'] = [i + 1 for i in range(linecount)]
    return logdf

def load_data(path,dataset):
    log_file=benchmark_settings[dataset]['log_file']
    log_format=benchmark_settings[dataset]['log_format']
    log_path=path+'/'+log_file
    # log_path=path+'/{}/{}.log'.format(dataset,dataset)
    headers, regex = generate_logformat_regex(log_format)
    df_log = log_to_dataframe(log_path, regex, headers)
    return df_log

def load_ground_truth(path,dataset):
    log_file="{}/{}/{}_2k.log_structured_corrected.csv".format(path,dataset,dataset)
    df_log=pd.read_csv(log_file)
    return df_log


def sample_logs(datas, num):
    candidates = random.sample(datas, 1)
    for i in range(num-1):
        sub_candidates = random.sample(datas,32)
        max_dis=0
        max_log=""
        for log in sub_candidates:
            sub_min_dis=10000000000
            for c in candidates:
                dis= Levenshtein.distance(log, c)
                if(sub_min_dis>=dis):
                    sub_min_dis=dis
            if(sub_min_dis>max_dis):
                max_dis=sub_min_dis
                max_log=log
        candidates.append(max_log)
    return candidates


def candidate_set_construction(path, dataset_now, num, Groups):
    all_logs=[]
    log_template_map={}
    if(not Groups):
        for dataset in datasets:
            if(dataset==dataset_now):
                continue
            else:
                df_log = load_ground_truth(path,dataset)
                for idx, line in df_log.iterrows():
                    all_logs.append(line['Content'])
                    log_template_map[line['Content']]=line['EventTemplate']
        candidates = sample_logs(all_logs, num)
        ret=[]
        for log in candidates:
            ret.append({"content":log, "template":log_template_map[log]})
        return ret
    else:
        GroupA=Groups[0]
        GroupB=Groups[1]
        if(dataset_now in GroupA):
            candidate_Group=GroupB
        else:
            candidate_Group=GroupA
        for dataset in datasets:
            if(dataset not in candidate_Group):
                continue
            else:
                df_log = load_ground_truth(path,dataset)
                for idx, line in df_log.iterrows():
                    all_logs.append(line['Content'])
                    log_template_map[line['Content']]=line['EventTemplate']
        candidates = sample_logs(all_logs, num)
        ret=[]
        for log in candidates:
            ret.append({"content":log, "template":log_template_map[log]})
        return ret

