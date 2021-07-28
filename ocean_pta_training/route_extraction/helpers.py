"""
In this module we define helper functions with minimal modifications) that used
in the original jupyter notebook, but that do not belong in the class (typically,
because the function does not depend on any of the in-memory data structures).
"""

import numpy as np
import pandas as pd
import sys
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from itertools import chain, count


def np_runlengths(seq, return_run_numbers=False, as_frame=False):
    """
    Returns (run_starts, run_lengths, run_vals, [run_nums]) and does not use
    any equality tolerance for floats. Does not work for NaN values (they will
    not form a run).  If arr is actually a tuple, it should consist of multiple
    sequences of the same length, and all values for a position must match.
    """
    if isinstance(seq, tuple):
        multi_match = True
        all_arrs = [np.asarray(x) for x in seq]
        arr, *other_arrs = all_arrs
    else:
        multi_match = False
        arr = np.asarray(seq)
    arr_len = len(arr)
    if arr_len == 0:
        no_ints = np.zeros(0, dtype=int)
        nums_ret = [no_ints] if return_run_numbers else []
        if multi_match:
            return (
                no_ints,
                no_ints,
                tuple(arr2[no_ints] for arr2 in all_arrs),
                *nums_ret
            )
        else:
            return no_ints, no_ints, arr[no_ints], *nums_ret
    unequal = (arr[1:] != arr[:-1])
    if multi_match:
        for arr2 in other_arrs:
            unequal |= (arr2[1:] != arr2[:-1])
    run_starts = np.r_[0, np.flatnonzero(unequal) + 1]
    run_lengths = np.diff(np.r_[run_starts, arr_len])
    if multi_match:
        vals = tuple(arr2[run_starts] for arr2 in all_arrs)
    else:
        vals = arr[run_starts]
    if return_run_numbers:
        run_nums = np.empty(len(arr), dtype=int)
        for i, runstart, runlen in zip(count(), run_starts, run_lengths):
            run_nums[runstart:runstart+runlen] = i
    if as_frame:
        if multi_match:
            coldict = {
                f"value{i+1}": v for i, v in enumerate(vals)
            }
            coldict['run_start'] = run_starts
            coldict['run_len'] = run_lengths
            df = pd.DataFrame(coldict)
        else:
            df = pd.DataFrame(dict(
                value=vals,
                run_start=run_starts,
                run_len=run_lengths
            ))
        if return_run_numbers:
            return df, run_nums
        else:
            return df
    elif return_run_numbers:
        return run_starts, run_lengths, vals, run_nums
    else:
        return run_starts, run_lengths, vals


def remove_consec_dup_elts(portlist):
    """
    Doesn't work for NaN in list, because NaN != NaN and we haven't made this
    more complicated... see also earlier function np_runlengths that
    returns a similar result in its third value
    """
    return [
        x for x, prev in zip(portlist, chain([None], portlist)) if x != prev
    ]


def expand_iloc_slice_list(slices):
    """
    <TODO>
    """
    if slices:
        return np.r_.__getitem__(tuple(slices))
    else:
        return np.array([], dtype=int)


def get_slice_len(slx):
    if isinstance(slx, slice):
        return len(range(*slx.indices(sys.maxsize)))
    elif isinstance(slx, np.ndarray) and slx.ndim == 1:
        # OK, that's not a slice, but it's accepted by
        # expand_iloc_slice_list so let's allow it
        return len(slx)
    else:
        raise ValueError(f"value type {type(slx).__name__} is not supported")


def days_between_ts(t1, t2):
    """
    works for scalars or series
    WARNING: uses non-imported class datetime.timediff
    """
    tdiff = t2 - t1
    if isinstance(tdiff, pd.Series):
        return tdiff.dt.total_seconds() / (24*3600)
    else:
        try:
            # uses non-imported class datetime.timediff
            return tdiff.total_seconds() / (24*3600)
        except:
            return np.nan


def add_lead_time_cols(df2, timechunks, jdurs):
    """
    <TODO>
    """
    slicelens = list(map(len, timechunks))
    starts = pd.Series(
        np.repeat([tchunk.iat[0] for tchunk in timechunks],
                  slicelens),
        index=df2.index)
    durations = np.repeat(jdurs, slicelens)
    times = pd.Series(
        np.concatenate(timechunks),
        index=df2.index
    )
    sofar = days_between_ts(starts, times)
    # division by zero here should be impossible unless the timestamps
    # are duplicated, but let's protect
    fracs = np.where(np.isclose(durations, 0), 0., sofar/durations)
    df2['remaining_lead_time'] = durations - sofar
    df2['journey_percent'] = fracs


def q95(x):
    return x.quantile(0.95)


def get_port_sequence(port_ser):
    """<TODO>"""
    runs = np_runlengths(port_ser.fillna(method='ffill'))
    all_ports = runs[2]
    return all_ports


def match_valid_port_sequence(df, od_port_sequence_valid, od_valid_stats, port_sequences_valid):
    """
    TODO: Documentation
    TODO: Unused arguments
    """
    invalid_port_sequence = df['port_sequence']
    od_invalid = df['OD']
    journey_time_mean = df['journey_time_mean']
    result = process.extractOne(
        invalid_port_sequence,
        od_port_sequence_valid[od_port_sequence_valid.OD == od_invalid]['port_sequence'].tolist(),
        scorer=fuzz.ratio
    )
    matched_valid_port_sequence = result[0]
    score = result[1]

    return pd.Series(
        [od_invalid, invalid_port_sequence, matched_valid_port_sequence, score, journey_time_mean],
        index=[
            'OD', 'invalid_port_sequence', 'matched_valid_port_sequence', 'score', 'journey_time_mean'
        ]
    )


def cleanse_port_sequence(od_df):
    """<TODO>"""
    port_sequences = (
        od_df
        .groupby(['OD', 'IMO', 'route_ID'], group_keys=False)
        ['mapped_stopped_closest_port'].apply(get_port_sequence).reset_index()
    )
    journey_times = (
        od_df
        .groupby(['OD', 'IMO', 'route_ID'], group_keys=False)
        .agg(journey_time=('remaining_lead_time', 'max'))
        .reset_index()
    )
    port_sequences.columns = ['OD', 'IMO', 'route_ID', 'port_sequence_list']
    port_sequences['port_sequence'] = port_sequences.apply(
        lambda row: '-'.join(row['port_sequence_list']), axis=1
    )
    port_sequences['num_intermediate_ports'] = port_sequences.apply(
        lambda row: len(row['port_sequence_list'])-2, axis=1
    )
    port_sequences = port_sequences.merge(journey_times, how='inner', on=['OD', 'IMO', 'route_ID'])
    od_port_sequence = (
        port_sequences[['OD', 'IMO', 'port_sequence', 'num_intermediate_ports', 'journey_time']]
        .groupby(['OD', 'port_sequence'])
        .agg(
            num_routes=('IMO', 'count'),
            num_intermediate_ports=('num_intermediate_ports', 'mean'),
            journey_time_max=('journey_time', 'max'),
            journey_time_min=('journey_time', 'min'),
            journey_time_mean=('journey_time', 'mean'),
            journey_time_median=('journey_time', 'median'),
            journey_time_95perc=('journey_time', q95)
        )
        .reset_index()
        .sort_values(
            by=['OD', 'num_routes', 'num_intermediate_ports'],
            ascending=False
        )
    )
    threshold_port_sequences = 2
    od_port_sequence_valid = od_port_sequence[od_port_sequence.num_routes >= threshold_port_sequences].copy()
    od_port_sequence_invalid = od_port_sequence[od_port_sequence.num_routes < threshold_port_sequences].copy()
    # print('Number of valid port sequences after threshold cut-off are: ',len(od_port_sequence_valid))
    # print('Number of invalid port sequences after threshold cut-off are: ',len(od_port_sequence_invalid))
    port_sequences_valid = port_sequences[port_sequences.port_sequence.isin(od_port_sequence_valid.port_sequence)]
    od_valid_stats = (
        port_sequences_valid[['OD', 'IMO', 'port_sequence', 'num_intermediate_ports', 'journey_time']]
        .groupby(['OD'])
        .agg(
            num_routes=('OD', 'count'),
            num_intermediate_ports=('num_intermediate_ports', 'mean'),
            journey_time_max=('journey_time', 'max'),
            journey_time_min=('journey_time', 'min'),
            journey_time_mean=('journey_time', 'mean'),
            journey_time_median=('journey_time', 'median'),
            journey_time_95perc=('journey_time', q95)
        )
        .reset_index()
        .sort_values(by=['OD', 'num_routes', 'num_intermediate_ports'], ascending=False)
    )
    od_valid_stats['journey_time_filter'] = od_valid_stats['journey_time_95perc']

    # TODO: remove redundant parentheses
    if ((len(od_port_sequence_invalid) > 0) & (len(od_port_sequence_valid) > 0)):

        matched_df: pd.DataFrame = (
            pd.DataFrame(
                od_port_sequence_invalid.apply(
                    match_valid_port_sequence,
                    axis=1,
                    args=[od_port_sequence_valid, od_valid_stats, port_sequences_valid]
                )
            )
            .sort_values(by='score', ascending=False)
        )
        matched_df = matched_df.merge(
            od_valid_stats[['OD', 'journey_time_filter']],
            how='inner',
            on='OD'
        )
        matched_df['journey_time_outlier_filter'] = 1.25 * matched_df['journey_time_filter']
        journey_time_filter = (matched_df.journey_time_mean <= matched_df.journey_time_filter)
        good_match_filter = (matched_df.journey_time_mean <= matched_df.journey_time_outlier_filter) & (matched_df.score >= 85)
        filtered_matched_df = matched_df[(journey_time_filter | good_match_filter)]
        valid_to_invalid = od_port_sequence_invalid[od_port_sequence_invalid.port_sequence.isin(filtered_matched_df.invalid_port_sequence)]
        od_port_sequence_valid = pd.concat([od_port_sequence_valid,valid_to_invalid])
        od_port_sequence_invalid = od_port_sequence_invalid.drop(valid_to_invalid.index)

        # print('Number of valid port sequences after recovering routes with slight variation are: ',len(od_port_sequence_valid));
        # print('Number of invalid port sequences after recovering routes with slight variation are: ',len(od_port_sequence_invalid));

    valid_port_sequences = port_sequences[
        port_sequences.port_sequence.isin(od_port_sequence_valid.port_sequence)
    ]
    od_df_valid = (
        od_df.merge(
            valid_port_sequences[['OD', 'IMO', 'route_ID', 'num_intermediate_ports', 'port_sequence']],
            how='inner', on=['OD', 'IMO', 'route_ID']
        )
        .reset_index(drop=True)
    )
    return od_df_valid,valid_port_sequences, od_port_sequence_valid
