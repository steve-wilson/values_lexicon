
import collections
import os
import time
import pickle
import random
import re
import traceback
import functools

from lxml import etree
import boto3
import xmltodict

import html

# mturk

# functions to create and grab results from mturk HITs
# should be able to be treated as a black box from the outside

MTURK_URL = "https://mturk-requester-sandbox.us-east-1.amazonaws.com"
TESTING = False
if not TESTING:
    MTURK_URL = 'https://mturk-requester.us-east-1.amazonaws.com'

REGION = 'us-east-1'

# lab
IAM_KEY = "<YOUR_IAM_KEY>"
SECRET_KEY = "<YOUR_SECRET_KEY>"

VERBOSE = True

class Connection(object):

    def __init__(self, iam_access_key=IAM_KEY, iam_secret_key=SECRET_KEY):

        rtp = True
        if TESTING:
            rtp = False

        self.client = boto3.client('mturk',
                aws_access_key_id = iam_access_key,
                aws_secret_access_key = iam_secret_key,
                region_name = REGION,
                endpoint_url = MTURK_URL)
        self.bonus_amount = "0.05"
        self.hit_type = self.client.create_hit_type(
                # This is 1 day
                AutoApprovalDelayInSeconds=86400,
                # This is 1 hour
                AssignmentDurationInSeconds=600,
                Reward="0.05",
                Title="Sort concepts into categories. (takes < 1 minute per HIT)",
                Keywords="Linguistics, Sorting, Quick, Words, Sort, Easy, Annotation, Semantics",
                Description="Given a set of words and/or short phrases, sort them into groups based on their meaning, then name the groups.",
                QualificationRequirements=[

                        # Require >= .95 approval rating
                        {'QualificationTypeId':"000000000000000000L0",
                        'Comparator':'GreaterThanOrEqualTo',
                        'IntegerValues':[95],
                        'RequiredToPreview':rtp},

                        # Require location=USA
                        {'QualificationTypeId':"00000000000000000071",
                        'Comparator':'EqualTo',
                        'LocaleValues':[{'Country':"US"}],
                        'RequiredToPreview':rtp}
                ]
            )

def make_connection():
    return Connection()

def make_hit(conn, groups, frameheight = 600):
    question_html = html.fill_template(groups, testing=TESTING)
    question = ("""<HTMLQuestion xmlns="http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2011-11-11/HTMLQuestion.xsd"> \n""" +
                "<HTMLContent><![CDATA[\n" + question_html + "]]>\n</HTMLContent>\n" +
                "<FrameHeight>" + str(frameheight) + "</FrameHeight>\n</HTMLQuestion>")
    try:
        hit = conn.client.create_hit_with_hit_type(HITTypeId=conn.hit_type['HITTypeId'],
                                                    MaxAssignments = 5,
                                                    LifetimeInSeconds = 604800,
                                                    Question = question)
    except Exception as e:
        print e.message
        raise e

    if VERBOSE:
        print "A new HIT has been created"
        print "https://worker.mturk.com/projects/" + hit['HIT']['HITGroupId'] + "/tasks?ref=w_pl_prvw"
#        print "https://mturk.com/mturk/preview?groupId=" + hit['HIT']['HITGroupId']
    return hit

def check_results(conn, hit):
    hit_id = hit["HIT"]["HITId"]
    submitted = conn.client.list_assignments_for_hit(HITId=hit_id, AssignmentStatuses=['Submitted'])
    results = submitted['Assignments']
    return results

def extend_hit(conn, hit, n):
    hit_id = hit["HIT"]["HITId"]
    r = conn.client.create_additional_assignments_for_hit(HITId=hit_id,
                                                            NumberOfAdditionalAssignments=n)

def find_val(xml_string):
    m = re.search(">([\w\s\d_]+)</div>",xml_string)
    res = None
    if m:
        res = m.group(1)
    return res

def read_table(html_table, loc="body/tbody"):
    table = etree.HTML(html_table).find(loc)
    rows = iter(table)
    headers = [col.text for col in next(rows)]
    result = collections.defaultdict(list)
    for row in rows:
        for i,val in enumerate([find_val(etree.tostring(col)) for col in row]):
            if val:
                result[headers[i]].append(val.split())
    return result

def parse_single_result(result):

    data ={}
    group_names = {}
    sorted_groups = []

    result['Answer'] = result['Answer'].replace('&lt;','<')
    result['Answer'] = result['Answer'].replace('&gt;','>')
    result['Answer'] = result['Answer'].replace('&#13;','')
    result['Answer'] = result['Answer'].replace('<meta http-equiv="Content-Type" content="text/html;charset=UTF-8">','')
    result['Answer'] = result['Answer'].replace('<br class="Apple-interchange-newline">','')
# quickfix for unclosed <tr> tags
    result['Answer'] = result['Answer'].replace('</td><tr><td>','</td></tr><tr><td>')

#    print result['Answer']

    try:
        xml_doc = xmltodict.parse(result['Answer'])
    except Exception as e:
        print "Problem Parsing:"
        print result['Answer']
        raise e
    question_form_answers = xml_doc['QuestionFormAnswers']

    for field_name, value_dicts in question_form_answers.items():
        if field_name == "Answer":
            for value_dict in value_dicts:
                if 'QuestionIdentifier' in value_dict:
                    if value_dict['QuestionIdentifier'] == "table-layout":

                        groups = []
                        tbody = value_dict['FreeText']['tbody']
                        for row in tbody['tr']:
                            if 'td' in row:
                                for i,item in enumerate(row['td']):
                                    if 'div' in item:
                                        if '#text' in item['div']:
                                            while i >= len(groups):
                                                groups.append([])
#                                            print groups
#                                            print i
                                            groups[i].append(item['div']['#text'].split())
                        sorted_groups = [g for g in groups if g]
#                        sorted_groups = read_table(value_dict["FreeText"])
                    elif re.match("g\d-name", value_dict["QuestionIdentifier"]):
                        free_text = value_dict["FreeText"]
                        if free_text:
                            free_text = free_text.strip().upper()
                            group_names[int(value_dict['QuestionIdentifier'].split('-')[0][1])+1] = free_text

    data['groups'] = sorted_groups
    data['labels'] = group_names
    data['aid'] = result['AssignmentId']
    data['wid'] = result['WorkerId']
#    print "DATA:",data
    return data

def wait_for_N_responses(conn, hit, N, delay=300):
    responses = []
    aids = set([])
    while len(responses) < N:
        results = check_results(conn, hit)
        print "NUM RESULTS:",len(results),len(responses),"out of",N,"(",hit['HIT']['HITId'],')'
        if results:
            for result in results:
                aid = result['AssignmentId']
                if aid not in aids:
                    data = parse_single_result(result)
                    responses.append(data)
                    aids.add(aid)
                    print "NEW RESULT:",data
                else:
                    if VERBOSE:
                        print "No new results, waiting", delay, "seconds..."
                        print
                    time.sleep(delay)
        else:
            if VERBOSE:
                print "No new results, waiting", delay, "seconds..."
                print
            time.sleep(delay)
    return responses


# Each response is a dict that has four keys:
# groups is the new list of groups
# labels is a dict mapping group number to label
# aid is the assignment ID (needed for rewards)
# wid is the worker ID (needed for rewards)
def check_agreement(responses, history):
    all_hgroups = []
    agree_worker_ids = []
    agree_assignment_ids = []

    for response in responses:

        groups = []
        labels = []

# old way
#        for group_name, group in response['groups'].items():
#            groups.append(group)
#            labels.append(response['labels'].get(int(group_name.split()[1]),""))

# new way
        groups = response['groups']
        for i in range(len(groups)):
            labels.append(response['labels'].get(i+1,""))

        print "GROUPS:",groups
        print "LABELS:",labels
        response['hgroups'], response['hlabels'] = make_hashable(groups, labels)
        all_hgroups.append(response['hgroups'])

    counts = collections.Counter(all_hgroups).most_common()
    options = [c[0] for c in counts if c[1] == counts[0][1]]
    print "Hgroups",all_hgroups,"Options", options
    most_common = None
    for option in options:
        in_lookup = history.contents.get(option, [])
        if in_lookup:
            most_common = option
            print "based on lookup in history, selected",most_common,"as the most common sorting"
    if not most_common:
        most_common = random.choice(options)
        print "tie-breaker: selected",most_common,"as the most common sorting"

    label_candidates = []
    for response in responses:

        if response['hgroups'] == most_common:
            agree_worker_ids.append(response['wid'])
            agree_assignment_ids.append(response['aid'])
            label_candidates.append(response['hlabels'])

    label_lists = []
    for i in range(len(label_candidates[0])):
        print i, len(label_candidates[0])
        label_list = []
        for label_candidate_set in label_candidates:
            label_list.append(label_candidate_set[i])
        label_lists.append(label_list)

    top_labels = []
    for label_list in label_lists:
        counts = collections.Counter(label_list).most_common()
        options = [c[0] for c in counts if c[1] == counts[0][1]]
        most_common_label = random.choice(options)
        top_labels.append(most_common_label)
    print "selected",top_labels,"as the labels for the most common sorting"

    return most_common, (agree_worker_ids, agree_assignment_ids), top_labels

def reverse_zip(list_of_pairs):
    l1,l2 = [],[]
    for pair in list_of_pairs:
        l1.append(pair[0])
        l2.append(pair[1])
    return l1,l2

# groups is a list of lists of strings (Each group is a list of strings)
# labels should be a list of strings in the same order as the groups
#   the only purpose of passing in labels is to get them sorted in the same order
def make_hashable(groups, labels=None):

    hashable = []
    for g in groups:
        hg = []
        for strings in g:
            hg.append(tuple(sorted(strings)))
        hashable.append(tuple(sorted(hg)) )
    # sort the labels in the same order that the groups are sorted
    if labels:
        groups_labels = sorted(zip(hashable, labels))
        hashable, labels = reverse_zip(groups_labels)
    else:
        hashable = sorted(hashable)
    return tuple(hashable), labels

def lookup_in_history(groups, path):

    result = ([],[])
    # create if it doesn't exist
    if not os.path.exists(path):
        with open(path,'w') as pklfile:
            history = {}
            pickle.dump(history, pklfile)
    else:
        with open(path,'r') as pklfile:
            history = pickle.load(pklfile)
            sgroups = []
            print groups
            for group in groups:
                sgroup = [g[1] for g in group]
                sgroups.append(sgroup)
            print sgroups
            result = history.get(make_hashable(sgroups)[0],([],[]))
    return result

def lookup_in_lookup(groups, lookup):
    sgroups = []
    for group in groups:
        sgroup = [g[1] for g in group]
        sgroups.append(sgroup)
    return lookup.get(make_hashable(sgroups),[])

def add_to_history(groups, new_groups, labels, path):

    history = None
    with open(path,'r') as pklfile:
        history = pickle.load(pklfile)
        sgroups = []
        print groups
        for group in groups:
            sgroup = [g[1] for g in group]
            sgroups.append(sgroup)
        print sgroups
        check = make_hashable(sgroups)[0]
        print check
        history[make_hashable(sgroups)[0]] = (new_groups, labels)
    with open(path,'w') as pklfile:
        pickle.dump(history, pklfile)

def send_reward(conn, workers_assignments, dont_reward=[], reason="You sorted the groups in the same way as a majority of other workers who completed the task! Nice!"):
    print "SENDING REWARD TO:",workers_assignments
    print "BUT NOT:",dont_reward
    workers_assignments = [wa for wa in workers_assignments if wa not in dont_reward]
    for i in range(len(workers_assignments[0])):
        worker_id = workers_assignments[0][i]
        assignment_id = workers_assignments[1][i]
        conn.client.send_bonus(WorkerId=worker_id,
                                BonusAmount=str(conn.bonus_amount),
                                AssignmentId=assignment_id,
                                Reason=reason)

MAX_STRINGS = 10

def trace_unhandled_exceptions(func):
    @functools.wraps(func)
    def wrapped_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            print "Exception in " + func.__name__
            traceback.print_exc()
    return wrapped_func

# needs to also take care of averaging the responses from multiple people
# output should be labels and the new groups that peple created (parallel arrays)
@trace_unhandled_exceptions
#def ask_user(groups, initial_responses = 5, add_responses = 4, lookup="lookup_12_25.pkl"):
def ask_user(groups, initial_responses = 5, add_responses = 4, lookup=""):

   
    connection = make_connection()
    new_groups = []
    labels = []
    new_group_nodes = []
    answers = []

    if lookup:
        lookup_table = pickle.load(open(lookup))
        answers = lookup_in_lookup(groups, lookup_table)
#answers = lookup_table[make_hashable(groups)]
        print "Found groups in history:",groups
    node_lookup = {}
    for group in groups:
        for node, strings in group:
            node_lookup[tuple(sorted(strings))] = node
    if new_groups:
        for new_group in new_groups:
            node_group = []
            for strings in new_group:
                node_group.append(node_lookup[tuple(sorted(strings))])
            new_group_nodes.append(node_group)

    else:
    
        abbreviated_groups = []
        abv2original= {}
        for group in groups:
            abv_group = []
            for node, strings in group:
                if len(strings) > MAX_STRINGS:
                    abv_strings = random.sample(strings,MAX_STRINGS)
                    abv2original[tuple(sorted(abv_strings))] = strings
                    abv_group.append(abv_strings)
                else:
                    abv_group.append(strings)
                    abv2original[tuple(sorted(strings))] = strings
            abbreviated_groups.append(abv_group)
        
        agreeing_workers = []
        n_agree_0 = 0
        if answers:
            most_common, agreeing_workers, top_labels = check_agreement(answers)
            n_agree_0 = len(agreeing_workers[0])
        if n_agree_0 > float(initial_responses)/2:
            print "Already enough responses"
            new_groups = most_common
            labels = top_labels
        else:
            dont_reward = agreeing_workers

            hit = make_hit(connection, abbreviated_groups)
            responses = wait_for_N_responses(connection, hit, initial_responses - len(answers))
            responses.extend(answers)
            most_common, agreeing_workers, top_labels = check_agreement(responses)
            n_agree = len(agreeing_workers[0])
            print "N agree:",n_agree
            if float(n_agree)/initial_responses > .5:
                new_groups = most_common
                labels = top_labels
                send_reward(connection, agreeing_workers, dont_reward)
            else:
                extend_hit(connection, hit, add_responses)
                responses2 = wait_for_N_responses(connection, hit, add_responses)
                most_common2, agreeing_workers2, top_labels2 = check_agreement(responses)
                n_agree2 = len(agreeing_workers2[0])
                print "N_Agree2:",n_agree2
                if n_agree2 == 1:
                    print "WARNING: max number of HITs created and no two people agreed!"
                    print "Options:",groups
                    print "Responses:"
                    print responses2
                    print "A random response was selected."
                elif float(n_agree2)/(initial_responses+add_responses) > .5:
                    send_reward(connection, agreeing_workers2, dont_reward)
                new_groups = most_common2
                labels = top_labels2

        full_string_groups = []
        for ng in new_groups:
            fsg = []
            node_group = []
            for strings in ng:
                orig_strings = abv2original[tuple(sorted(strings))]
                fsg.append(orig_strings)
                node_group.append(node_lookup[tuple(sorted(orig_strings))])
            full_string_groups.append(fsg)
            new_group_nodes.append(node_group)
        new_groups = full_string_groups

    print "FINAL SORTING:"
    print new_groups
    print new_group_nodes
    print labels

    return new_group_nodes, labels
