from argparse import ArgumentParser
from joblib import Parallel, delayed
from multiprocessing import Manager
from os import listdir, mkdir, path
import pandas as pd
from shutil import rmtree
from typing import Any, Iterator, Tuple
from xml.etree.ElementTree import iterparse


class DiscogsXMLParser:
    def __init__(self, input_path: str, genre: str):
        self.context: Iterator[Tuple[str, Any]]
        self.event = ""
        self.elem = None
        self.root = None
        self.input_path = input_path
        self.genre = genre
        self.metadata_fields = {"master_id", "released", "country"}
        self.exc = {"194", "355", "118760"}  # excluded artists: various/unknown/no artist, respectively
        self.data = []
        self.item = {}
        self.is_relevant = True
        self.in_description = False
        self.in_style = False
        self.in_artist = False
        self.artists = set()
        self.styles = set()
        self.seen_genres = set()
        self.skip_tags = {"images", "name", "anv", "join", "role", "tracks", "title", "labels", "extraartists", "notes",
                          "data_quality", "position", "duration", "identifiers", "videos", "companies"}

    def _init_parse(self):
        self.data = []
        self.context = iter(iterparse(self.input_path, events=("start", "end")))
        self.event, self.root = self.context.__next__()

    def _reset_state(self):
        self.item = {}
        self.is_relevant = True
        self.in_description = False
        self.in_style = False
        self.in_artist = False
        self.artists.clear()
        self.styles.clear()
        self.seen_genres.clear()

    def _set_state(self):
        if self.elem.tag == "styles":
            self.in_style = self.event == "start"
        elif self.elem.tag == "artist":
            self.in_artist = self.event == "start"
        elif self.elem.tag == "descriptions":
            self.in_description = self.event == "start"
        elif self.elem.tag == "genre" and self.event == "end":
            self.seen_genres.add(self.elem.text)
        elif self.elem.tag == "genres" and self.event == "end" and self.genre not in self.seen_genres:
            self.is_relevant = False

    def _update_top_level_info(self):
        if self.elem.tag in self.metadata_fields:
            self.item[self.elem.tag] = self.elem.text

    def _update_state_specific_info(self):
        if self.in_description and self.elem.tag == "description" and self.elem.text == "Compilation":
            self.is_relevant = False
        elif self.in_artist and self.elem.tag == "id" and self.elem.text is not None and self.elem.text not in self.exc:
            self.artists.add(self.elem.text)
        elif self.in_style and self.elem.tag == "style" and self.elem.text is not None:
            self.styles.add(self.elem.text)

    def _push_data(self):
        if self.is_relevant:
            self.item["id"] = self.elem.attrib["id"]
            self.item["styles"] = ", ".join(self.styles)
            self.item["artists"] = ", ".join(self.artists)
            self.data.append(self.item)

        self.elem.clear()
        self.root.clear()

    def _process_element(self):
        self._set_state()
        self._update_top_level_info()
        self._update_state_specific_info()

    def _skip_element(self):
        tag = self.elem.tag
        while not (self.elem.tag == tag and self.event == "end"):
            self.event, self.elem = self.context.__next__()

    def _process_or_skip_element(self):
        if self.elem.tag not in self.skip_tags:
            self._process_element()
        elif self.elem.tag != "release":
            self._skip_element()

    def _skip_release(self):
        while not (self.elem.tag in {"release", "releases"} and self.event == "end"):
            self.event, self.elem = self.context.__next__()

    def _parse(self):
        self._init_parse()

        for self.event, self.elem in self.context:
            if self.event == "start" and self.elem.tag == "release":
                self._reset_state()
            if self.is_relevant:
                self._process_or_skip_element()
            else:
                self._skip_release()
            if self.event == "end" and self.elem.tag == "release":
                self._push_data()

    def extract_sample(self, size: int, out_path: str):
        with open(self.input_path, "r") as in_f, open(out_path, "w") as out_f:
            data = []
            for i, line in enumerate(in_f):
                if i > size:
                    break
                data.append(line)

            out_f.writelines(data)

    def to_tsv(self, output_path: str = None):
        self._parse()
        pd.DataFrame(self.data).to_csv(output_path, sep="\t", index=False)

    def to_list(self):
        self._parse()
        return self.data


class ParallelParser:
    def __init__(self, input_path: str, genre: str, seg_size: int = 2000000):
        self.input_path = input_path
        self.genre = genre
        self.cur_seg_no = 1
        self.seg_data = []
        self.seg_size = seg_size
        self.end_seek = False
        self.cur_seg_file = None
        self.data = Manager().list()

    def _write_seg(self):
        self.cur_seg_file.writelines(self.seg_data)
        self.cur_seg_file.close()
        self.seg_data = []

    def _finish_seg(self):
        if self.cur_seg_no > 1:
            self.seg_data.insert(0, "<releases>\n")
        self.seg_data.append("</releases>")
        self._write_seg()
        self.cur_seg_no += 1
        self.end_seek = False
        self.cur_seg_file = open(path.join("tmp", f"seg-{self.cur_seg_no}.xml"), "w")

    def _process_xml_line(self, line_no: int, line: str):
        self.seg_data.append(line)
        if self.end_seek and line.endswith("</release>\n"):
            self._finish_seg()
        if line_no == self.cur_seg_no * self.seg_size:
            self.end_seek = True

    def _finish_partition(self):
        self.seg_data.insert(0, "<releases>\n")
        self.cur_seg_file.writelines(self.seg_data)
        self.cur_seg_file.close()
        self.cur_seg_file = None

    def _partition(self):
        if not path.isdir("tmp"):
            mkdir("tmp")
        with open(self.input_path, "r") as in_f:
            self.cur_seg_file = open(path.join("tmp", f"seg-{self.cur_seg_no}.xml"), "w")
            for line_no, line in enumerate(in_f):
                self._process_xml_line(line_no, line)
            self._finish_partition()

    def _process_seg(self, seg_path: str):
        if seg_path.endswith(".xml"):
            self.data.extend(DiscogsXMLParser(path.join("tmp", seg_path), self.genre).to_list())

    def _parse(self, do_cleanup: bool = True):
        Parallel(n_jobs=-1)(delayed(self._process_seg)(f) for f in listdir("tmp"))
        if do_cleanup:
            rmtree("tmp")

    def to_tsv(self, output_path: str = None):
        self._partition()
        self._parse()
        pd.DataFrame(list(self.data)).to_csv(output_path, sep="\t", index=False)


if __name__ == "__main__":
    parser = ArgumentParser("extract_data")
    parser.add_argument("xml_path", type=str, help="Path to the Discogs releases XML file")
    args = parser.parse_args()

    ParallelParser(args.xml_path, "Hip Hop").to_tsv("releases_raw.tsv")
