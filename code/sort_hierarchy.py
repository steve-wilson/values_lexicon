
import sys
from multiprocessing import Pool
import time

import hierarchy
import mturk

import functools
import traceback

# sort_hierarchy
# Steve Wilson
# Fall 2017

# top level script that automates the human powered sorting of the
#   hierarchy.

#MTURK = mturk.make_connection()

def sort_hierarchy(hierarchy_graph_file, outfile='manually-sorted.graph'):
    h = hierarchy.Hierarchy(hierarchy_graph_file)
    h = sort(h, h.root)
    h.write(outfile)

def get_all_decendents_in_level_order(h, node):
    q = [node]
    s = []
    while q:
        n = q.pop(0)
        s.append(n)
        for c in h.children[n]:
            q.append(c)
    return s

def get_strings_for_nodes(h, groups):
    group_strings = []
    for g in groups:
        group_strings.append( [(n,h.get_string(n)) for n in g] )
    return group_strings


def trace_unhandled_exceptions(func):
    @functools.wraps(func)
    def wrapped_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            print "Exception in " + func.__name__
            traceback.print_exc()
    return wrapped_func

@trace_unhandled_exceptions
def process_node(h, node):

    children = h.children[node]
    
    print "Running merge for nodes:",children
#    print "Current tree is:"
#    print h.to_graph()
    with open('latest_tree.graph','w') as cur_tree_file:
        cur_tree_file.write(str(h.to_graph()))
    print "updated latest_tree.graph file."

    initial_groups = []
    for child in children:
        grandchildren = h.children[child]
        if grandchildren:
            initial_groups.append(grandchildren)
        else:
            initial_groups.append([child])

    print initial_groups
    total_items = 0
    sorted_groups = []
    finished_groups = set([])
    labels = []
    for ig in initial_groups:
        total_items += len(ig)
    if total_items <= 2:
        print "no need to merge 1 or 2 items"
        sorted_groups = initial_groups
        finished_groups = {j for j in range(len(sorted_groups))}
        labels = ["" for k in range(len(sorted_groups))]
    else:
        str_igs = get_strings_for_nodes(h, initial_groups)

# MANUAL
        sorted_groups, labels = mturk.ask_user(str_igs)
#        sorted_groups, labels = mturk.TESTask_user(str_igs)

        remerge = True
        if len(sorted_groups) == 1:
            all_leaves = True
            for n in sorted_groups[0]:
                if h.children[n]:
                    all_leaves = False
            if all_leaves:
                remerge = False
        if remerge:
            for i,sg in enumerate(sorted_groups):
                two_leaves = len(sg)==2 and not any([h.children[n] for n in sg])
                if len(sg) <= 1 or sg in initial_groups or two_leaves:
                    finished_groups.add(i)
        else:
            finished_groups = {j for j in range(len(sorted_groups))}

    return sorted_groups, finished_groups, labels

def sort(h, root, n_proc=50, delay=600):

    process_stack = get_all_decendents_in_level_order(h, root)
    pool = Pool(processes=n_proc)
    results = {}
    nothing_new = False
    nodes = []

    while process_stack:

        print process_stack
        print "Nodes being processed now:",nodes

        # add nodes to process in parallel
        # don't add parents of any of these nodes, since we don't want to change a parent and child at the same time

        # make sure to clean up results so we don't keep checking results that have already been processed
        # doing this here so as not to modify the results dict while iterating through it
        results_nodes = results.keys()
        for node in results_nodes:
            if node not in nodes:
                results.pop(node)

        if len(nodes)==n_proc:
            print "No nodes ready to read in main loop. Will resume in",delay,"seconds."
            print
            time.sleep(delay)
        for i in range(n_proc-len(nodes)):
            next_node = process_stack.pop(-1)
            while not h.children[next_node]:
                next_node = process_stack.pop(-1)
            descendents = get_all_decendents_in_level_order(h, next_node)
            if any([descendent in nodes for descendent in descendents]):
                if nothing_new:
                    print "Getting ahead of ourselves! Can't process node",next_node,"yet. Sleeping for",delay,"seconds."
                    time.sleep(delay)
                process_stack.append(next_node)
                break
            nodes.append(next_node)

        # parallelize this so all nodes are processed at once
        for node in nodes:
            if node not in results:
                print "processing node",node
# MANUAL
                results[node] = pool.apply_async(process_node, [h,node])
#            results[node] = process_node(h, node)
        nothing_new = True
        for node,result in results.items():
# MANUAL
            if result.ready():
                nothing_new = False
                nodes.remove(node)
                sorted_groups, finished_groups, labels = result.get()
#            sorted_groups, finished_groups, labels = result
                print "SGs:",sorted_groups
                print "FGs:",finished_groups
                print "LABs:",labels
                for child in h.children[node]:
                    if h.children[child]:
                        print "deleting node",child
                        h.delete_node(child)
                h.children[node] = set([])
                for i,sg_label in enumerate(zip(sorted_groups,labels)):
                    sg,label = sg_label
                    if len(sg) > 1:
                        newnode = h.get_next_id()
                        h.children[newnode] = set(sg)
                        h.node_attributes[newnode]['label'] = label
                        h.children[node].add(newnode)
                        if i not in finished_groups:
                            process_stack.append(newnode)
                    else:
                        h.children[node].add(list(sg)[0])
                        if 'label' not in h.node_attributes[node]:
                            h.node_attributes[node]['label'] = label
#                        if 0 not in finished_groups:
#                            process_stack.append(node)

    return h

if __name__ == "__main__":
    sort_hierarchy(sys.argv[1], mturk.make_connection())
