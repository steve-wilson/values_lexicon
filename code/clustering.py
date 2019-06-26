
import collections
import sys
import time

import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import AgglomerativeClustering
from sklearn import manifold

from scipy.cluster.hierarchy import dendrogram

vec_file = sys.argv[1]
labs = []
X = []
with open(vec_file) as vecs_in:
    header = vecs_in.readline()
    for line in vecs_in.readlines():
        lab, x = line.strip().split(' ',1)
        X.append( [float(xx) for xx in x.split()] )
        labs.append(lab)
        
X = np.mat(X)
X = np.array(X)
X_red = manifold.SpectralEmbedding(n_components=2).fit_transform(X)
cmap_name = 'nipy_spectral'
cmap = plt.get_cmap(cmap_name)
colors = [cmap(i) for i in np.linspace(0,1,100)]
print len(colors)

saved_clustering = None
    
n_clusters = 100
linkage = 'ward'
affinity='euclidean'
clustering = AgglomerativeClustering(linkage=linkage, n_clusters=n_clusters, affinity=affinity)
clustering.fit(X)
saved_clustering = clustering

# write the tree to graphviz format
branches = []
label_dict = collections.defaultdict(list)
text = []
n_samples = len(saved_clustering.labels_)

for i in range(n_samples):
    text.append( (str(i), '"' + labs[i] + '"') )
#for i,label in enumerate(saved_clustering.labels_):
#    label_dict[label].append(labs[i])
#for l,words in label_dict.items():
#    text.append( (l,'"' + r'\n'.join(words) +'"') )

for i in range(len(saved_clustering.children_)):
    node_num = n_samples + i
    pair = saved_clustering.children_[i]
    branches.append( (node_num,pair[0]) )
    branches.append( (node_num,pair[1]) )
    
with open("hierarchy.graph",'w') as graph_file:
    # open
    graph_file.write("digraph G {\n")
    # draw all of the connections
    for branch in branches:
        graph_file.write('\t' + str(branch[0]) + ' -> ' + str(branch[1]) + ';\n')

    # write the words in the leaf nodes
    for label_text in text:
        graph_file.write('\t' + str(label_text[0]) + ' [label=' + label_text[1] + '];\n')
    # close
    graph_file.write('}')
