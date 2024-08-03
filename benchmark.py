from logparser.dataloader import benchmark_settings
from evaluation import evaluate
import pandas as pd

parsed_path = "results/qwen2-72B/"


bechmark_result=[]
for dataset, setting in benchmark_settings.items():
    print('\n=== Evaluation on %s ===' % dataset)
    result_path = parsed_path + dataset + '_2k.log_structured.csv'
    ground_path = "logs/" + dataset + '/' + dataset + '_2k.log_structured_corrected.csv'
    try:
        GA, PA, PTA, RTA = evaluate(ground_path, result_path)
        print(dataset, GA, PA, PTA, RTA)
        bechmark_result.append([dataset, GA, PA, PTA, RTA])
        print('\n=== Overall evaluation results ===')
    except:
        print(dataset)
df_result = pd.DataFrame(bechmark_result, columns=['Dataset', 'GA', 'PA', 'PTA', 'RTA'])
print(df_result)
df_result.T.to_csv(parsed_path + 'bechmark_result.csv')


