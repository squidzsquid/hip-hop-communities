"""
    Created on 11/feb/2015
    @author: Giulio Rossetti
"""
import csv
from datetime import datetime
import gzip
from networkx import Graph


class TILES(object):
    """
        TILES
        Algorithm for evolutionary community discovery
    """

    def __init__(self, filename=None, obs=7, path=""):
        """
            Constructor
            :param filename: Path to the edges file (TSV)
            :param obs: observation window (days)
            :param path: Path specifying where to generate the results
        """
        self.filename = filename
        self.obs = obs
        self.path = path
        self.start = None
        self.g = Graph()
        self.cid = 0
        self.slice_no = 0
        self.communities = {}
        self.dt_map = {}

    @staticmethod
    def batched(records, size=50000):
        """ Generator function to yield batches of n records at a time

            Args:
                records(iterable): An iterable containing the records to be batched
                size(int): The number of records to be included in each batch

            Yields:
                range, list: The start/end record indices, and the actual records
        """

        idx = 0
        batch = records[idx: idx + size]

        while len(batch) > 0:
            yield range(idx, idx + len(batch)), batch
            idx += len(batch)
            batch = records[idx: idx + size]

    def execute(self):
        """
            Execute TILES algorithm
        """
        with open(self.filename, "r") as f:
            self.start = f.readline().split("\t")[2].rstrip()
            self.dt_map = {t.rstrip(): datetime.fromtimestamp(int(t)) for t in {x.split("\t")[2] for x in f}}

        last_break = self.dt_map[self.start]

        #################################################
        #                   Main Cycle                  #
        #################################################
        with open(self.filename) as f:
            tsv_reader = csv.reader(f, delimiter="\t")
            for line in tsv_reader:
                u = int(line[0])
                v = int(line[1])
                dt = self.dt_map[line[2]]

                #############################################
                #               Observations                #
                #############################################
                if (dt - last_break).days >= self.obs:
                    last_break = dt
                    print("New slice. Starting Day: %s" % dt)
                    self.print_communities()

                if u == v:
                    continue

                if not self.g.has_node(u):
                    self.g.add_node(u, c_coms=set())  # central

                if not self.g.has_node(v):
                    self.g.add_node(v, c_coms=set())

                try:
                    self.g.adj[u][v]["weight"] += 1
                    continue
                except KeyError:
                    self.g.add_edge(u, v, weight=1)

                u_n = self.g[u]
                v_n = self.g[v]

                #############################################
                #               Evolution                   #
                #############################################

                # new community of peripheral nodes (new nodes)
                if len(u_n) > 1 and len(v_n) > 1:
                    self.common_neighbors_analysis(u, v, list(set(u_n) & set(v_n)))

        self.print_communities()

    @property
    def new_community_id(self):
        """
            Return a new community identifier
            :return: new community id
        """
        self.cid += 1
        self.communities[self.cid] = set()
        return self.cid

    def common_neighbors_analysis(self, u, v, common_neighbors):
        """
            General case in which both the nodes are already present in the net.
            :param u: a node
            :param v: a node
            :param common_neighbors: common neighbors of the two nodes (list of (name, {node}) tuples)
        """

        # no shared neighbors
        if len(common_neighbors) < 1:
            return
        else:
            v_node = self.g.nodes[v]["c_coms"]
            u_node = self.g.nodes[u]["c_coms"]
            v_coms = {*v_node}
            u_coms = {*u_node}
            shared_coms = v_coms & u_coms
            only_u = u_coms - v_coms
            only_v = v_coms - u_coms

            # community propagation: a community is propagated iff at least two of [u, v, z] are central
            common_neighbors_coms = []
            propagated = False

            for z in common_neighbors:
                z_node = self.g.nodes[z]["c_coms"]
                common_neighbors_coms.append((z, z_node))

                if only_u or only_v:
                    for c in z_node:
                        if c in only_v:
                            self.add_to_community(u, u_node, c)
                            propagated = True
                        elif c in only_u:
                            self.add_to_community(v, v_node, c)
                            propagated = True

                for c in shared_coms:
                    if c not in z_node:
                        self.add_to_community(z, z_node, c)
                        propagated = True

            if not propagated:
                # new community
                actual_cid = self.new_community_id
                self.add_to_community(u, u_node, actual_cid)
                self.add_to_community(v, v_node, actual_cid)

                for z, z_node in common_neighbors_coms:
                    self.add_to_community(z, z_node, actual_cid)

    def print_communities(self):
        """
            Print the actual communities
        """

        out_file_coms = gzip.open("%s/strong-communities-%d.gz" % (self.path, self.slice_no), "wt", 3)
        nodes_to_coms = {}
        coms_to_remove = []
        drop_c = []

        for idc, comk in self.communities.items():
            if self.communities[idc] is not None:
                if len(comk) > 2:
                    com = [*comk]
                    com.sort()
                    key = tuple(com)

                    # Collision check and merge index build (maintaining the lowest id)
                    if key not in nodes_to_coms:
                        nodes_to_coms[key] = idc
                    else:
                        old_id = nodes_to_coms[key]
                        drop = idc

                        if idc < old_id:
                            drop = old_id
                            nodes_to_coms[key] = idc

                        # merged to remove
                        coms_to_remove.append(drop)

                else:
                    drop_c.append(idc)
            else:
                drop_c.append(idc)

        for _, batch in self.batched(list(nodes_to_coms.items())):
            out_file_coms.writelines([u"%d\t%s\n" % (b[1], str(list(b[0]))) for b in batch])

        out_file_coms.close()

        for dc in drop_c:
            self.destroy_community(dc)

        # write the graph
        out_file_graph = gzip.open("%s/graph-%d.gz" % (self.path, self.slice_no), "wt", 3)
        out_file_graph.writelines([u"%d\t%s\t%d\n" % (e[0], e[1], e[2]) for e in self.g.edges.data("weight")])
        out_file_graph.close()

        # community cleaning
        for c in coms_to_remove:
            self.destroy_community(c)

        self.slice_no += 1

    def destroy_community(self, cid):
        nodes = [*self.communities[cid]]  # slightly faster than doing list(dict.keys())
        for n in nodes:
            n_node = self.g.nodes[n]["c_coms"]
            self.remove_from_community(n, n_node, cid)
        del self.communities[cid]  # n.b. "cid in self.communities" checked pre-call

    def add_to_community(self, node_name, node, cid):
        node.add(cid)
        if cid in self.communities:
            self.communities[cid].add(node_name)
        else:
            self.communities[cid] = {node_name, }

    def remove_from_community(self, node_name, node, cid):
        if cid in node:
            node.remove(cid)
            if node_name in self.communities[cid]:
                self.communities[cid].remove(node_name)
