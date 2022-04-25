library("sankeyD3")
library("DynCommPhylo")


#original_network <- network_construction(f = "edges.tsv", years = c(1979:1990))
# load("network.rds")

load_network <- function(){
  links <- read.csv("network_links.tsv", sep="\t")
  nodes <- read.csv("network_nodes.tsv", sep="\t")
  nodes$individuals <- lapply(nodes$individuals, function(x) unlist(strsplit(x, ", ")))
  list("links"=links, "nodes"=nodes)
}
network <- load_network()

simplified_network <- network_simplification(links = network$links, nodes = network$nodes)
save(simplified_network, file = "simplified_network_new.rds")
