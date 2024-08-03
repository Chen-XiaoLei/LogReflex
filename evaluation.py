from collections import Counter
import pandas as pd
import re
import scipy.special


def get_TA(EIDS, ground_truth, parser, length):
    parsermap = {}
    groundmap = {}
    df_parser = pd.read_csv(parser)
    # df_parser = pd.read_csv(parser, quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
    df_groundtruth = pd.read_csv(ground_truth)

    for idx, line in df_parser.iterrows():
        eid = line['EventId']
        template = line['EventTemplate']
        parsermap[eid] = template
    for idx, line in df_groundtruth.iterrows():
        eid = line['EventId']
        template = line['EventTemplate']
        groundmap[eid] = template

    truth_count = 0
    truth_count_PA = 0
    turthtemplate = []
    errortemplate = []
    for (parserEid, groundEid, count) in EIDS:

        template_parser = parsermap[parserEid]
        template_ground_truth = groundmap[groundEid]

        Equivalence_situation = [["\w+_<\*>", "<*>"], ["<\*>, <\*> - <\*>, <\*>", "<*>"], ["<\*>/<\*>", "<*>"],
                                 ["/<\*>", "<*>"], ["<\*>:<\*>", "<*>"], ["<\*>]", "<*>"],
                                 ["<\*>>", "<*>"], ["<\*>##<\*>", "<*>"], ["<\*> ms", "<*>"], ["MININT-<\*>", "<*>"],
                                 ["0[xX]<\*>", "<*>"], ["<\*>,", "<*>"], ["<\*>;", "<*>"], ["<\*>-<\*>", "<*>"],
                                 ["<\*>[MKG]B", "<*>"], ["<\*>\.<\*>", "<*>"], ["<\*>\.", "<*>"], ["core\.<\*>", "<*>"],
                                 ["node-<\*>", "<*>"], [":<\*>", "<*>"], ["<\*>:", "<*>"], ["CPU<\*>", "<*>"],
                                 ["IRQ<\*>", "<*>"], ["\"<\*>\"", "<*>"], ["v<\*>", "<*>"], ["\s\s", " "],["<\*>\.0", "<*>"],
                                 ["\+<\*>", "<*>"], ["BP-<\*>", "<*>"], ["<\*>Kbytes", "<*>"], ["#<\*>#", "<*>"],
                                 ["\(<\*>\)", "<*>"], ["<\*> <\*>", "<*>"], ["<\*> us", "<*>"], ["<\*> - <\*>", "<*>"],
                                 ["<\*>0", "<*>"], ["<\*>\|<\*>", "<*>"], ["<\*>ms", "<*>"], ["<\*>@<\*>", "<*>"],
                                 ["#<\*>", "<*>"], ["<\*><\*>", "<*>"], ["proxy\.<\*>", "<*>"], ["@<\*> @<\*>", "@<*>"],
                                 ["<\*> bytes <\*>", "<*> "], ["<\*>:", "<*>"], ["-<\*>", "<*>"], ["MININT-<*>", "<*>"],
                                 ["<\*>M", "<*>"], ["<\*>rts", "<*>"], ["<\*>C", "<*>"], ["https:<\*>", "<*>"],
                                 ["job<\*>", "<*>"],["<\*> [MGK]B", "<*>"],["\\\<\*>", "<*>"],["<\*> bytes", "<*>"],
                                 ["<<\*>", "<*>"], ["<\*>,", "<*>"], ["<\*>GHz", "<*>"], ["<\*>K", "<*>"],
                                 ["task<\*>", "<*>"],["<\*> seconds", "<*>"],["blk_<\*>", "<*>"], ["<\*>\w+", "<*>"],
                                 ["<\*> <\*>", "<*>"], ["0x<\*>", "<*>"], ["blk<\*>", "<*>"], ["attempt<\*>", "<*>"],
                                 ["container<\*>", "<*>"], ["<\*><\*>", "<*>"], ["<\*>,", "<*>"]]
        Equivalence_template = [{"parser": "Alarm uploadStaticsToDB totalSteps=<*>Calories:<*>Floor:<*>Distance:<*>",
                                 "groundtruth": "Alarm uploadStaticsToDB totalSteps=<*>:<*>:<*>:<*>"},
                                {"parser": "next day:steps<*>mLastReport<*>",
                                 "groundtruth": "next day:<*>"}, ]
        conti = False
        for condition in Equivalence_template:
            if (template_parser == condition['parser'] and template_ground_truth == condition['groundtruth']):
                truth_count += 1
                truth_count_PA += count
                turthtemplate.append([parsermap[parserEid], groundmap[groundEid]])
                conti = True
                break
        if (conti):
            continue
        if(type(template_parser)!=str):
            continue
        for (before, after) in Equivalence_situation:
            while (re.findall(before, template_parser)):
                template_parser = re.sub(before, after, template_parser)
            while (re.findall(before, template_ground_truth)):
                template_ground_truth = re.sub(before, after, template_ground_truth)

        if (template_parser == template_ground_truth):
            truth_count += 1
            truth_count_PA += count
            turthtemplate.append([parsermap[parserEid], groundmap[groundEid]])

        else:
            errortemplate.append([parsermap[parserEid], groundmap[groundEid], count])

    PTA = (truth_count * 1.0) / (len(parsermap.keys()) * 1.0)
    RTA = (truth_count * 1.0) / (len(groundmap.keys()) * 1.0)
    PA = (truth_count_PA * 1.0) / (1.0 * length)
    return PTA, RTA, PA


def get_accuracy(series_groundtruth, series_parsedlog, debug=False):
    EIDS = []
    series_groundtruth_valuecounts = series_groundtruth.value_counts()
    real_pairs = 0
    for count in series_groundtruth_valuecounts:
        if count > 1:
            real_pairs += scipy.special.comb(count, 2)

    series_parsedlog_valuecounts = series_parsedlog.value_counts()
    parsed_pairs = 0
    for count in series_parsedlog_valuecounts:
        if count > 1:
            parsed_pairs += scipy.special.comb(count, 2)
    accurate_pairs = 0
    accurate_events = 0  # determine how many lines are correctly parsed
    for parsed_eventId in series_parsedlog_valuecounts.index:
        logIds = series_parsedlog[series_parsedlog == parsed_eventId].index
        series_groundtruth_logId_valuecounts = series_groundtruth[logIds].value_counts()
        error_eventIds = (parsed_eventId, series_groundtruth_logId_valuecounts.index.tolist())
        error = True
        if series_groundtruth_logId_valuecounts.size == 1:
            groundtruth_eventId = series_groundtruth_logId_valuecounts.index[0]
            if logIds.size == series_groundtruth[series_groundtruth == groundtruth_eventId].size:
                accurate_events += logIds.size
                EIDS.append([parsed_eventId, groundtruth_eventId, len(logIds)])
                error = False
        if error and debug:
            print('(parsed_eventId, groundtruth_eventId) =', error_eventIds, 'failed', logIds.size, 'messages')
        for count in series_groundtruth_logId_valuecounts:
            if count > 1:
                accurate_pairs += scipy.special.comb(count, 2)

    GA = float(accurate_events) / series_groundtruth.size

    return EIDS, GA


def evaluate(groundtruth, parsedresult):
    df_groundtruth = pd.read_csv(groundtruth)
    # df_parsedlog = pd.read_csv(parsedresult, index_col=False, quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
    df_parsedlog = pd.read_csv(parsedresult, index_col=False)
    # Remove invalid groundtruth event Ids
    null_logids = df_groundtruth[~df_groundtruth['EventId'].isnull()].index
    df_groundtruth = df_groundtruth.loc[null_logids]
    df_parsedlog = df_parsedlog.loc[null_logids]

    EIDS, GA = get_accuracy(df_groundtruth['EventId'], df_parsedlog['EventId'])
    length = len(df_parsedlog['EventId'])
    # print('Precision: %.4f, Recall: %.4f, F1_measure: %.4f, accuracy_PA: %.4f'%(precision, recall, f_measure, accuracy_GA))
    PTA, RTA, PA = get_TA(EIDS, groundtruth, parsedresult, length)

    return GA, PA, PTA, RTA
