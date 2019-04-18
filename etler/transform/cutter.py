from etler.load.logger import LoggingStream
import pandas as pd


def chunk(
    filename,
    chunkdir,
    chunksize,
    sep=",",
    chunk_prefix="chunk",
    index=False,
    should_log=True,
    logname="bad_lines.log",
):
    chunkdir.mkdir(parents=True, exist_ok=True)
    log = open(logname, "w") if should_log else None
    with LoggingStream(log, log):
        for idx, chunk in enumerate(
            pd.read_csv(
                filename,
                sep=sep,
                chunksize=chunksize,
                error_bad_lines=False,
                warn_bad_lines=True,
            )
        ):
            chunk.to_csv(chunkdir / f"{chunk_prefix}_{idx}.csv", index=index)
