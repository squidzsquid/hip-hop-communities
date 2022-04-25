from datetime import datetime
from itertools import chain
import json
from os import path
from typing import Dict, List, Set


class Network:
    def __init__(self, edges_file: str, min_group_size: int = 1, min_weight: int = 1):
        self.edges: Dict[int, Dict[str, List[int]]] = {}
        self._load_edges(edges_file)
        self.years: Set[int]
        self.graph_i: List[Dict[str, int]] = []
        self.coms_i: Dict[str, List[int]] = {}
        self.min_group_size: int = min_group_size
        self.min_weight: int = min_weight
        self._c = {}  # communities for current year
        self._c_prev = None  # communities for previous year
        self._nd = {}  # network densities for current year communities
        self._graph_nodes_lookup: Dict[int, set] = {}
        self._links = []
        self._nodes = []
        self._construct()

    def _reset(self):
        self._c = {}
        self._c_prev = {}
        self._links = []
        self._nodes = []

    def _add_edge(self, year: int, node1: int, node2: int):
        if year not in self.edges:
            self.edges[year] = {"node1": [], "node2": []}
        self.edges[year]["node1"].append(node1)
        self.edges[year]["node2"].append(node2)

    def _load_edges(self, edges_file: str):
        self.edges = {}
        ts_lookup = {}

        with open(edges_file) as f:
            for x in f:
                line = x.rstrip().split("\t")
                if line[2] in ts_lookup:
                    year = ts_lookup[line[2]]
                else:
                    year = datetime.fromtimestamp(int(line[2])).year
                    ts_lookup[line[2]] = year
                self._add_edge(year, int(line[0]), int(line[1]))

        self.years = sorted([*self.edges])

    def _load_coms(self, i: int, year: int):
        self.coms_i = {}

        with open(path.join("output", f"strong-communities-{i}")) as f:
            for x in f:
                line = x.rstrip().split("\t")
                self.coms_i[f"{line[0]}_{year}"] = json.loads(line[1])

    def _load_graph(self, i: int):
        self.graph_i = []

        with open(path.join("output", f"graph-{i}")) as f:
            for x in f:
                line = x.rstrip().split("\t")
                self.graph_i.append({"node1": int(line[0]), "node2": int(line[1])})

    def _start_new_year(self, year: int, file_no: int):
        if file_no > 0:
            self._c_prev = self._c

        self._load_coms(file_no, year)
        self._load_graph(file_no)
        self._c = {}
        self._build_graph_nodes_lookup()
        self._nd = {}

    def _add_node(self, a: int, b: int):
        if a not in self._graph_nodes_lookup:
            self._graph_nodes_lookup[a] = {b, }
        else:
            self._graph_nodes_lookup[a].add(b)

    def _build_graph_nodes_lookup(self):
        self._graph_nodes_lookup = {}

        for n in self.graph_i:
            self._add_node(n["node1"], n["node2"])
            self._add_node(n["node2"], n["node1"])

    def _expand_coms(self):
        """Expand community to include peripheral members (i.e. include n1 AND n2 if either are found)"""
        for com, members in self.coms_i.items():
            new_com = dict.fromkeys(members)
            for m in members:
                for p in self._graph_nodes_lookup[m]:
                    if p not in new_com:
                        new_com[p] = None

            self._c[com] = new_com

            ncommon = len([n for n in self.graph_i if n["node1"] in self._c[com] and n["node2"] in self._c[com]])
            self._nd[com] = ncommon / ((len(self._c[com]) * (len(self._c[com]) - 1)) / 2)

    def _assign_members_to_single_coms(self, year: int):
        """Assign individuals in multiple communities to the community with highest nd (ties broken by group size)"""
        individuals = dict.fromkeys(chain.from_iterable(self._c.values()))
        stragglers = set(individuals).difference(set(self.edges[year]["node1"]) | set(self.edges[year]["node2"]))

        for ind in individuals:
            matches = [k for k, v in self._c.items() if ind in v]

            if ind in stragglers:
                for match in matches:
                    del self._c[match][ind]

            elif len(matches) > 1:
                matches.sort(key=lambda x: (self._nd[x], len(self._c[x])), reverse=True)
                match = matches[0]

                for m in matches:
                    if m != match:
                        del self._c[m][ind]

    def _remove_too_small_coms(self):
        """Remove groups smaller than the minimum group size"""
        self._c = {com: members for com, members in self._c.items() if len(members) >= self.min_group_size}

    def _store_output(self):
        if self._c_prev:
            temp_links = []
            for com_prev, members_prev in self._c_prev.items():
                for com, members in self._c.items():
                    ncommon = len(set(members_prev.keys()) & set(members.keys()))
                    if ncommon >= self.min_weight:
                        temp_links.append((com_prev, com, ncommon))

            self._links.extend(temp_links)
        self._nodes.extend([(k, ", ".join(map(str, v))) for k, v in self._c.items()])

    def _construct(self):
        self._reset()

        for i, year in enumerate(self.years):
            print(f"Starting year: {year}")
            self._start_new_year(year, i)
            self._expand_coms()
            self._assign_members_to_single_coms(year)
            self._remove_too_small_coms()
            self._store_output()

    def save(self, links_file: str = "network_links.tsv", nodes_file: str = "network_nodes.tsv"):
        with open(links_file, "w") as f:
            f.writelines(["from\tto\tvalue\n"] + ["\t".join(map(str, link)) + "\n" for link in self._links])
        with open(nodes_file, "w") as f:
            f.writelines(["community\tindividuals\n"] + ["\t".join(node) + "\n" for node in self._nodes])


if __name__ == "__main__":
    Network("edges.tsv").save()
