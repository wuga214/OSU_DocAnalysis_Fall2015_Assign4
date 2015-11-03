'''
Created on Nov 1, 2015

@author: Wuga, Liping Liu
'''
# you need to install networkx first with pip
import networkx as nx
import matplotlib.pyplot as plt
import sys
import csv

if len(sys.argv) < 2:
    print("usage: python plot_graph.py <edge_list_file>")
    exit()

G=nx.Graph()

# read edges from the file
edge_file = sys.argv[1]
with open(edge_file) as f:
       elist = list(csv.reader(f))

# edge weight is read as string, change it to float  
elist = [(a[0], a[1], float(a[2])) for a in elist]

# form the graph  
G.add_weighted_edges_from(elist)

# calculate centrality 
Node= nx.betweenness_centrality(G, weight="weight")

# get graph layout   
pos = nx.spring_layout(G)

# Set node size with centrality
Size= [x*5000 for x in Node.values()]

# plot the graph 
plt.figure(1,figsize=(12,10))
nx.draw_networkx_nodes(G,pos,alpha=0.2,node_size=Size,node_color=Size)
nx.draw_networkx_edges(G,pos,alpha=0.1)
nx.draw_networkx_labels(G,pos,font_size=8,font_color='b')
plt.show()
