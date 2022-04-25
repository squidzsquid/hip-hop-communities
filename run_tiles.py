import cProfile
from os import mkdir, path
from pstats import SortKey, Stats
from faster_tiles import TILES


def run_tiles(edges_path: str, output_dir: str = "output") -> None:
    if not path.isdir(output_dir):
        mkdir(output_dir)

    TILES(edges_path, path=output_dir, obs=365).execute()


if __name__ == "__main__":
    run_tiles("edges.tsv")
    # cProfile.run('run_tiles("edges.tsv")', 'prof')
    # p = Stats('prof')
    # p.sort_stats(SortKey.CUMULATIVE).print_stats(10)

