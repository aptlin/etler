from pathlib import Path
from contextlib import redirect_stderr
import sys

import pandas as pd


def chunk(
    filename: str,
    chunkdir: str,
    chunksize: int,
    sep=",",
    chunk_prefix="chunk",
    index=False,
    should_log=True,
    logname="chunking_errors.log",
):
    """Slices the dataset into chunks.

    Arguments:
        filename {str} -- The name of the csv file.
        chunkdir {str} -- The name of the directory storing the chunks.
        chunksize {int} -- The number of lines to store in a chunk.

    Keyword Arguments:
        sep {str} -- the dataset separator (default: {","})
        chunk_prefix {str}
            the chunk filename prefix. (default: {"chunk"})
        index {bool}
            determines whether the index is added to chunks (default: {False})
        logname {str}
            The name of the log with problematic lines.
            None or "" if logging is disabled. (default: {"bad_lines.log"})
    """
    datadir = Path(chunkdir)
    datadir.mkdir(parents=True, exist_ok=True)
    log = open(logname, "w") if logname else sys.stderr
    with redirect_stderr(log):
        for idx, chunk in enumerate(
            pd.read_csv(
                filename,
                sep=sep,
                chunksize=chunksize,
                error_bad_lines=False,
                warn_bad_lines=True,
            )
        ):
                chunk.to_csv(datadir / f"{chunk_prefix}_{idx}.csv", index=index)
    log.close()
