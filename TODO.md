# TODO

* ~~Parse XML to produce TSV output~~
* ~~TSV output must match up with the example rdata file~~
* ~~Needs ID, year, country, list of styles, list of artists~~
  * ~~Find where to get artists info + as integer~~
  * ~~Find out how to exclude compilations, and do so~~
  * ~~Exclude "extraartists"~~
  * ~~Profile parser code and speed up if poss~~
  * ~~Write code to convert releases TSV to edge list~~
* ~~Produce network via TILES for Python~~
* ~~Import this network into R, along with the original TSV~~
* ~~Export original_network and simplified network object for ease of re-running~~
* ~~Import releases dataset into R and ensure it matches releases.RData exactly~~
* Label up properly and export figures
* Document the whole process and tidy up code etc.
* Re-run workflow with latest data and including tracklist

# Extra todo:
* XML file is enormous and unwieldy, BaseX did NOT work at all (did the original researchers just load the whole thing into RAM and query it?) -- write bespoke, iterative parser -- takes about one hour to run (n.b. maybe need to include tracklist artists though -- compare and see difference)?
* Tidy up resulting releases.tsv
* Pass this to TILES -- much quicker than suggested in the paper... (n.b. must use networkx version 2.3, which has a bug in dag.py -- gcd import needs to be changed to just use math module)
* Download R packages from github - sankeyD3 and DynCommPhylo
* Run R code (takes a good few hours/run overnight)

# Run steps
* Retrieve releases XML from http://data.discogs.com/?prefix=data/2021/
* Unzip XML file
* Run extract_data.py to extract hip hop releases as TSV (passing the path to the XML file as an argument)
* Run format_data.py to clean up the data and run the TILES algorithm

500k line files -> started running at 15:15 - 15:3? (25 mins) -- 400 files
750k line files -> started running at 17:23 - 17:4? (23 mins) -- 300 files 
1m line files -> started running at 14:47 - 15:11 (24 mins) -- 200 files
2m line files -> strated running at 16:00 - 16:22 (21) mins -- 100 files

23 seconds (< 1986)
