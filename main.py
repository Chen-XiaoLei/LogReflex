from logparser.LogReflex import LogParser
import random

datasets = ['HDFS', 'Hadoop', 'Spark', 'Zookeeper', 'OpenStack', 'BGL', 'HPC', 'Thunderbird',
            'Windows', 'Linux', 'Mac', 'Android', 'HealthApp', 'Apache', 'Proxifier', 'OpenSSH']

GroupA = random.sample(datasets, 8)
GroupB=[]
for dataset in datasets:
    if(dataset not in GroupA):
        GroupB.append(dataset)

for dataset in datasets:
    parser = LogParser(dataset=dataset, indir="logs", outdir="results/qwen2-72B", cache_path="Cache/qwen2-72B", k=3, candidate_num=32, Groups=[GroupA, GroupB])
    parser.parse()

