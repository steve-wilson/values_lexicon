
# object used to represent a hierarchy
# should be able to load and export from dot format

import collections
import random
import sys

import mturk

VERBOSE = True

class Hierarchy(object):

    def __init__(self, dotfile=None):
        self.graph_type = ""
        self.graph_name = ""
        self.children = collections.defaultdict(set)
        self.node_attributes = collections.defaultdict(dict)
        self.root = None
        self.used_nodes = set([])
        self.seed = 48104
        if dotfile:
            self.load(dotfile)

    def load(self, dotfile):
        all_parents = set([])
        all_children = set([])
        with open(dotfile) as dot:
            self.graph_type, self.graph_name, _ = dot.readline().split()
            for line in dot.readlines():
                if '->' in line:
                    nodeA, nodeB = line.strip().strip(';').split('->')
                    nodeA, nodeB = nodeA.strip(), nodeB.strip()
                    self.children[nodeA].add(nodeB)
                    all_parents.add(nodeA)
                    all_children.add(nodeB)
                elif 'label' in line:
                    node, att_val = line.strip().split(" ",1)
                    att,val = att_val.strip('[];').split('=',1)
                    self.node_attributes[node][att] = val.strip('"')
        root_candidates = all_parents - all_children
        # there should only be one root!
        assert(len(root_candidates) == 1)
        self.root = list(root_candidates)[0]
        self.used_nodes = all_parents.union(all_children)
        self.next_id = max(list([int(x) for x in self.used_nodes])) + 1

    def get_children(self, node):
        return self.children[node]

    def is_descendent(self, a, b):
        candidates = list(self.children[b])
        while candidates:
            cand = candidates.pop(0)
            if cand == a:
                return True
            candidates.extend(list(self.children[cand]))

    def to_graph(self, number=False, labels=True):
        outstr = ""
        outstr += self.graph_type + ' ' + self.graph_name + ' {\n'
        for parent, children in self.children.items():
            for child in list(children):
                outstr += '\t' + parent + ' -> ' + child + ';\n'
        for node, attributes in self.node_attributes.items():
            for attribute, value in attributes.items():
                num = ""
                if number and attribute == 'label':
                    num = str(node) + '. '
                if not labels and value.isupper():
                    value = ""
                outstr += '\t' + node + ' [' + attribute + '="'+ num + value + '"];\n'
        outstr += '}'
        return outstr

    def write(self, outfile):
        with open(outfile,'w') as out:
            out.write(self.to_graph())

    def get_string(self, node, use_subset=False):
        if not self.children[node]:
            return [self.node_attributes[node].get('label')]
        else:
            elts = []
            for child in self.children[node]:
                elts.extend(self.get_string(child))
            if use_subset:
                elts = random.sample(elts, min(10,len(elts)))
            return elts

    def get_next_id(self):
        i =  str(self.next_id)
        self.next_id += 1
        return i

    def delete_node(self, n):
        if n in self.children:
            self.children.pop(n)
        if n in self.node_attributes:
            self.node_attributes.pop(n)
        if n in self.used_nodes:
            self.used_nodes.remove(n)

    def clear_capital_labels(self, att='label'):
        for node,atts in self.node_attributes.items():
            if att in atts:
                if self.node_attributes[node][att].isupper():
                    self.node_attributes[node].pop(att)

    def get_all_leaves(self,start=None):
        if not start:
            start = self.root
        leaves = []
        to_process = list(self.children[start])
        while to_process:
            node = to_process.pop(0)
            if self.children[node]:
                to_process.extend(list(self.children[node]))
            else:
                leaves.append(node)
        return leaves

    def collapse_leaf_children(self):
        to_process = list(self.children[self.root])
        while to_process:
            node = to_process.pop(0)
            if self.children[node]:
                if not any([len(self.children[child]) > 0 for child in self.children[node]]):
                    self.collapse_node(node)
                else:
                    to_process.extend(list(self.children[node]))

    # only do this for nodes with leaf children. otherwise, behavior is undefined.
    def collapse_node(self, node):
        child_strings = [self.node_attributes[child].get('label') for child in self.children[node]]
        self.node_attributes[node]['label'] = ' '.join(child_strings)
        for child in self.children[node]:
            self.delete_node(child)
        self.children[node] = set([])

    def update(self, node, new_organization):
        new_groups, new_labels = new_organization
        original_groups, lookup = self.to_string_groups_and_lookup(node, True)
        # only need this line for replay.py to work correctly
        __ , full_lookup = self.to_string_groups_and_lookup(node, False)
        original_groups, _ = mturk.make_hashable(original_groups)
        needs_to_be_resorted, add_to_sorted = [], []
        children = self.children[node]
        if VERBOSE:
            print "Original groups:",original_groups
            print "Original children:",children
        for child in children:
            if self.children[child]:
                self.delete_node(child)
                if VERBOSE:
                    print "removing node:",child
        new_children = []
        if len(new_groups) > 1:
            for group,label in zip(new_groups, new_labels):
                if VERBOSE:
                    print "New group:",group,"Label:",label
                if len(group) > 1:
                    newnode = self.get_next_id()
                    if VERBOSE:
                        print "Creating node:",newnode
                    self.used_nodes.add(newnode)
                    # will this work?
                    try:
                        self.children[newnode] = [lookup[item] for item in group]
                    except:
                        newnode_children = []
                        for item in group:
                            for key,value in full_lookup.items():
                                if set(item).issubset(set(key)):
                                    newnode_children.append(value)
                        self.children[newnode] = newnode_children
#                        print "Newnode's children:", newnode_children
#                        print "Group:", group
#                        print "Lookup:"
#                        print full_lookup
#                        ___ = raw_input("Press enter to continue...")
                        assert(len(newnode_children)==len(group))
                    if VERBOSE:
                        print "children of newnode:",self.children[newnode]
                    self.node_attributes[newnode]['label'] = label
                    new_children.append(newnode)
                    if group not in original_groups:
                        if VERBOSE:
                            print "group wasn't in original groups"
                        needs_to_be_resorted.append(newnode)
                    else:
                        add_to_sorted.append(newnode)
                else:
                    if VERBOSE:
                        print "Group has length 1 or 0."
                    single_node = None
                    try:
                        single_node = lookup[group[0]]
                    except:
                        item = group[0]
                        for key,value in full_lookup.items():
                            if set(item).issubset(set(key)):
                                single_node = value
                    assert(single_node != None)
                    new_children.append(single_node)
                    if 'label' not in self.node_attributes[single_node]:
                        self.node_attributes[single_node]['label'] = label
                        if VERBOSE:
                            print "setting label of node to",label
            if VERBOSE:
                print "new children:",new_children
            self.children[node] = new_children
        else:
            if VERBOSE:
                print "all items were placed into a single group. Setting as direct children of node..."
            group, label = new_groups[0],new_labels[0]
            if VERBOSE:
                print "New group",group,"Label:",label
            self.children[node] = [lookup[item] for item in group]
            if VERBOSE:
                print "New children of node:",self.children[node]
                print "no need to re-sort anything."
        return needs_to_be_resorted, add_to_sorted

    def all_siblings_in_set(self, node, s):
        parent = self.parent_of(node)
        siblings = self.children[parent]
        return all([sibling in s for sibling in siblings])

    # somewhat inefficient, but won't be called too often to majorly slow things down
    # could also implement self.children as a 2-way dictionary
    def parent_of(self, node):
        for parent, children in self.children.items():
            if node in children:
                return parent
        # shouldn't happen unless node==self.root
        return None

    def to_groups(self, node):
        groups = []
        if self.children[node]:
            for child in self.children[node]:
                if self.children[child]:
                    groups.append([grandchild for grandchild in self.children[child]])
                else:
                    groups.append([child])
            groups.append([self.get_string(c) for c in children])
        else:
            groups = self.get_string(node)
        return groups

    def to_string_groups_and_lookup(self, node, abv=False, seed=None):

        if not seed:
            random.seed(self.seed)
        else:
            random.seed(seed)

        groups = []
        node_lookup = {}
        if self.children[node]:
            for child in self.children[node]:
                if self.children[child]:
                    new_group = []
                    for grandchild in self.children[child]:
                        grandchild_string = self.get_string(grandchild, abv)
                        new_group.append(grandchild_string)
                        node_lookup[tuple(sorted(grandchild_string))] = grandchild
                    groups.append(new_group)
                else:
                    child_string = self.get_string(child, abv)
                    node_lookup[tuple(sorted(child_string))] = child
                    groups.append([child_string])
#            groups.append([self.get_string(c, abv) for c in children])
        else:
            groups = [self.get_string(node, abv)]
        return groups, node_lookup
