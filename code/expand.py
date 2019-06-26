
import collections
import sys
sys.path.append("../sorting")
import hierarchy

from gensim.models import KeyedVectors

FT = False
if FT:
    from gensim.models import FastText

# expand all nodes in the hierarcy, stopping after resolving an expansion collisions

def expand(hierarchy_path, emb, nodes_to_print=None, max_expansion=100):

# 0. load hierarchy
    h = hierarchy.Hierarchy(hierarchy_path)

# 1. get seed set for each node
    node2seeds = {}
    to_process = [h.root]
    while to_process:
        next_node = to_process.pop(0)
        node2seeds[next_node] = [x.split()[1] for x in h.get_string(next_node)]
        to_process.extend(list(h.children[next_node]))

# 2. get expansion candidates for each seed set, including distances to seed set center
    node2cands = {}
    cand2nodes = collections.defaultdict(list)
    for node, seeds in node2seeds.items():
        seeds_with_spaces = []
        for seed in seeds:
            if FT:
                if '_' in seed:
                    seed = seed.replace('_',' ')
                seeds_with_spaces.append(seed)
            else:
                if '_' in seed or '-' in seed:
                    if seed not in emb.vocab:
                        parts = []
                        if '_' in seed:
                            parts = seed.split('_')
                        if '-' in seed:
                            parts = seed.split('-')
                        seeds_with_spaces.extend(parts)
                    else:
                        seeds_with_spaces.append(seed)
                elif seed not in emb.vocab:
                    pass
                else:
                    seeds_with_spaces.append(seed)
        if seeds_with_spaces:
            cands = emb.most_similar(seeds_with_spaces,topn=max_expansion)
            print "Seeds:",seeds[:5]
            print "Cands:",cands[:5]
            node2cands[node] = [x[0] for x in cands]
            for seed in seeds:
                single_item_list = [seed]
                not_found = False
                if FT:
                    if '_' in seed:
                        single_item_list = seed.split('_')
                else:
                    if seed not in emb.vocab:
                        if '_' in seed:
                            single_item_list = seed.split('_')
                        elif '-' in seed:
                            single_item_list = seed.split('-')
                        else:
                            not_found = True
                if not_found:
                    pass
                else:
                    cand2nodes[seed].append((node,emb.n_similarity(single_item_list, seeds_with_spaces)))
            for cand in cands:
                cand2nodes[cand[0]].append((node,cand[1]))

# 3. from i to max, look at the ith item from each expanded set to see if there is a collision.
    final_node2cands = {}
    for i in range(max_expansion/2):
        for node, cands in node2cands.items():
            if i < len(cands):
                current = cands[i]
                collisions = cand2nodes[current]
                if len(collisions) > 1:

# 4. for each collision, assign to closest seed set center, and set to be removed from all non-ancestors/non-descendents
                    collisions.sort(key=lambda x:x[1],reverse=True)
                    if collisions[0][0] != node:
                        # check if closest node is ancenstory/descendent
                        if not h.is_descendent(collisions[0][0],node) and not h.is_descendent(node,collisions[0][0]):

# 5. after the first collisions for each set that was claimed by a different node (non ancestor/descendent), stop expanding the node
                            print current,"was closer to",h.node_attributes[collisions[0][0]].get('label','unknown'),"than it was to",h.node_attributes[node].get('label','unknown'),". Stopping expansion for node #",h.node_attributes[node].get('label','unknown')
#print node2cands[node][max(i-10,0):min(i+10,len(node2cands[node]))]
                            node2cands[node][i] = ""
# 6. (optional) return sets of words that belong to a pre-selected set of nodes
    print "Printing results:"
    print "================="
    print
    if nodes_to_print:
        nlist = [x.strip() for x in open(nodes_to_print).readlines()]
        for n in nlist:
            strings = node2seeds.get(n,['<No Seeds Found>']) + node2cands.get(n,['<No Expansion>'])
            strings = [s for s in strings if s]
            print n,h.node_attributes[n]['label'].split()[1],' '.join(list(set(strings)))

if __name__ == "__main__":
    emb_loc = sys.argv[1]
    emb = KeyedVectors.load_word2vec_format(emb_loc, binary=False)
    sorted_graph_file = sys.argv[2]
    measureable_nodes_file = sys.argv[3]
    expand(sorted_graph_file, emb, measurable_nodes_file)
