"""
Performs OD route extraction over a pre-determined set of routes
"""
import h3.api.basic_int as h3
import os
import numpy as np
import logging
import json
import pandas as pd
import re
import yaml
from collections import defaultdict
from itertools import chain
from haversine import haversine_vector, Unit
from typing import Dict, List, Optional, Tuple, Union
from .constants import (
    CONFIG_FILE_DEFAULT_FILENAME, IMO,
    JOBS, JOB_NAME, JOB_ORIGIN, JOB_DESTINATION,
    JOURNEY_BREAKER, OUTPUT_TRAINING_FILE_SUBDIR, OUTPUT_STATS_SUBDIR,
    MAPPED_PORT, PORT, RANGE_START, RANGE_LENGTH, TIME_POSITION
)
from .data_objects import VesselPortSequence
from .helpers import (
    add_lead_time_cols, cleanse_port_sequence, expand_iloc_slice_list,
    get_slice_len, np_runlengths, days_between_ts
)
from .port_codes import PORT_LETTER_CHARS, JOURNEY_BREAKER_LETTER
from .regex import intermed_port_chars_admissible


# Numeric constants that parameterize the algorithm
# TODO: Make these configurable
DISTANCE_FROM_PORT_THRESHOLD_FOR_ARRIVED: int = 15  # should this be a float?
MINIMUM_ROUTE_OBSERVATIONS_FOR_INCLUSION: int = 3
VESSEL_SPEED_THRESHOLD_FOR_STOPPED: float = 0.5

# TODO: Describe this
VesselPortSequence.EMPTY = VesselPortSequence("", {}, np.array([], dtype=int))


class OriginDestinationRouteExtractor(object):
    """
    Performs OD route extraction over a pre-determined set of routes.
    """
    jobs: List
    successful_jobs: List
    failed_jobs: List

    output_root_dir: str
    training_file_output_dir: str
    output_stats_dir: str

    # DECLARE VARIOUS DATA STRUCTURES NEEDED FOR THIS PROCEDURE

    # Loaded from file
    edited_ports_df: pd.DataFrame
    vessel_movements_df: pd.DataFrame
    od_list_df: pd.DataFrame

    # Computed
    port_to_latlon: Dict
    port_to_mapped_port: Dict
    hex5_to_possible_ports: Dict
    imo_range_df: pd.DataFrame
    imo_to_main_range_start: Dict
    imo_to_digested_port_sequence: Dict

    is_movement_data_sorted: bool

    def __init__(self,
                 path_to_ports_file: str,
                 path_to_vessel_movements_data: str,
                 path_to_od_file: str,
                 path_to_output_dir: str,
                 config_path: Optional[str] = None):
        """
        NOTE: Executing the constructor will automatically load the historical
        vessel movements data. This will consume significant memory. It is
        recommended that the caller not instantiate this class more than once,
        but this guideline is not enforced (e.g. through a singleton design)
        """

        self.logger = logging.getLogger(f"{__name__}.{__class__.__name__}")

        # You can optionally overwrite default configs to specify
        #  which routes to extract and/or configure various settings
        self.config = self.set_config(config_path)

        self.manage_local_file_dirs(path_to_output_dir)
        self.set_jobs()

        # Load data structures from external files
        self.edited_ports_df = self.load_ports_file(path_to_ports_file)
        self.vessel_movements_df = self.load_vessel_movements_dataframe(path_to_vessel_movements_data)
        self.is_movement_data_sorted = False

        # Compute calculated data structures
        self.set_port_to_mapped_port()
        self.load_od_list_df(path_to_od_file)

    def run(self):
        """
        Run the feature extraction procedure on the required origin-destination routes.
        """
        # Create derived data structures needed to run write_all_od_subframes
        self.set_port_latlon_dict()
        self.mark_hexes_near_ports()
        self.compute_stopped_nearest_port_fields()
        self.make_imo_range_data()
        self.compute_imo_to_digested_port_sequence()

        # Run
        self.write_all_od_subframes(
            main_df=self.vessel_movements_df,
            name_list=list(map(lambda j: j.get(JOB_NAME), self.jobs)),
            od_list=list(map(
                lambda j: (j.get(JOB_ORIGIN), j.get(JOB_DESTINATION)),
                self.jobs
            )),
            route_threshold_od=MINIMUM_ROUTE_OBSERVATIONS_FOR_INCLUSION
        )
        self.log_successful_and_failed_jobs()
        self.log_metrics()
        self.write_success_failure_json_files()

    def log_metrics(self):
        """Log a message to info level describing counts/stats"""
        pass  # TODO

    def log_successful_and_failed_jobs(self):
        """
        Log messages to info level describing which jobs succeeded and which failed.
        """
        success_msg = f"The following feature feature extraction jobs were SUCCESSFUL: "
        success_msg = f"{success_msg}{json.dumps(self.successful_jobs, indent=3)}"
        success_msg = f"\n{success_msg}\n"  # add some padding
        self.logger.info(success_msg)

        failure_msg = f"\nThe following feature feature extraction jobs were UNSUCCESSFUL: "
        failure_msg = f"{failure_msg}{json.dumps(self.failed_jobs, indent=3)}"
        failure_msg = f"\n{failure_msg}\n"  # add some padding
        self.logger.info(failure_msg)

    def write_success_failure_json_files(self):
        """
        We write success/failure jobs to JSON file
        (write_all_od_subframes) already writes the same data to CSV
        """
        success_file_name = os.path.join(self.output_root_dir, "successful_jobs.json")
        failure_file_name = os.path.join(self.output_root_dir, "failed_jobs.json")

        with open(success_file_name, "w") as success_json_file:
            json.dump(self.successful_jobs, success_json_file, indent=3)

        with open(failure_file_name, "w") as failed_json_file:
            json.dump(self.failed_jobs, failed_json_file, indent=3)

    def manage_local_file_dirs(self, local_output_dir: str):
        """
        Ensure output directories exist; create them, if necessary
        """
        self.output_root_dir = local_output_dir
        if not os.path.isdir(self.output_root_dir):
            os.makedirs(self.output_root_dir)

        self.training_file_output_dir = os.path.join(self.output_root_dir, OUTPUT_TRAINING_FILE_SUBDIR)
        if not os.path.isdir(self.training_file_output_dir):
            os.mkdir(self.training_file_output_dir)

        self.output_stats_dir = os.path.join(self.output_root_dir, OUTPUT_STATS_SUBDIR)
        if not os.path.isdir(self.output_stats_dir):
            os.mkdir(self.output_stats_dir)

    def set_jobs(self):
        """
        Read in jobs from configs. This defines the list of routes for
        which we will try to extract and write out training files
        """
        self.logger.info("Reading in the list of jobs from configurations")

        jobs_from_config: Dict = self.config.get(JOBS)
        if jobs_from_config:
            self.jobs = [
                {
                    JOB_NAME: name,
                    JOB_ORIGIN: nodes[JOB_ORIGIN],
                    JOB_DESTINATION: nodes[JOB_DESTINATION]
                }
                for name, nodes in jobs_from_config.items()
            ]
        else:
            self.logger.warning("There are no jobs to process in the config file.")

        self.successful_jobs = []
        self.failed_jobs = []

    def compute_imo_to_digested_port_sequence(self):
        """
        <TODO>
        """
        self.logger.info(f"Computing IMO to digested port series...")
        self.imo_to_digested_port_sequence = {

            imo: self.create_vessel_port_sequence(
                vessel_df=self.vessel_movements_df.iloc[start_row:start_row + n_rows],
                port_col=MAPPED_PORT,
                journey_breaker_col=None
            )
            for imo, start_row, n_rows in zip(self.imo_range_df[IMO],
                                              self.imo_range_df[RANGE_START],
                                              self.imo_range_df[RANGE_LENGTH])
        }

    def write_all_od_subframes(self,
                               main_df: pd.DataFrame,
                               name_list: List[str],
                               od_list: List[Tuple[str, str]],
                               route_threshold_od: int = 3):
        """
        We implement this, because get_all_od_subframes was defined
        in the notebook, but never used. This one was used to write output
        files (see blocks 70-72)

        od_list existed in the original notebook; it and name_list could be replaced by simply
        iterating through self.jobs. name_list and od_list are a transformation of the items in jobs.
        """
        self.logger.info("ATTEMPTING TO EXTRACT TRAINING DATA FOR ALL ORIGIN-DESTINATION PAIRS IN JOBS")

        # If this condition holds, then all of the journey tracing is by mapped OD, so it's useless
        # TODO: Need more documentation on this exit condition.
        if all(self.port_to_mapped_port.get(orig, orig) == self.port_to_mapped_port.get(dest, dest)
               for orig, dest in od_list):
            return

        success_odlist = []
        failed_odlist = []
        for idx, (orig, dest) in enumerate(od_list):
            name = name_list[idx]
            slicelist = []
            routeidlist = []
            odlist = []
            timechunklist = []
            jdurlist = []
            routebase = 1

            self.logger.info(f"The movement extraction process started for: {orig}-{dest}")

            for vessel_imo, range_start in self.imo_to_main_range_start.items():
                ret1 = self.get_vessel_od_subframe(
                    main_df,
                    vessel_imo,
                    range_start,
                    orig,
                    dest,
                    return_slices_only=True,
                    return_journey_starts_only=False,
                    add_lead_times=True
                )
                if ret1:
                    slices, tchunks, jdurs, od = ret1
                    slicelist.extend(slices)
                    odlist.append(od)
                    routeidlist.extend(range(routebase, routebase + len(slices)))
                    timechunklist.extend(tchunks)
                    jdurlist.extend(jdurs)

            if slicelist:
                num_slices = len(slicelist)
            else:
                num_slices = 0

            self.logger.info(f"The number of routes stitched for this OD are: {num_slices}")

            if num_slices > route_threshold_od:
                flattened_odlist = list(chain(*odlist))
                od_df: pd.DataFrame = (
                    self.vessel_movements_df.iloc[expand_iloc_slice_list(slicelist)]
                    .assign(
                        OD=np.repeat(flattened_odlist, list(map(get_slice_len, slicelist))),
                        route_ID=np.repeat(routeidlist, list(map(get_slice_len, slicelist)))
                    )
                )
                add_lead_time_cols(od_df, timechunklist, jdurlist)
            else:
                # TODO: Documentation (and maybe a message) related to this null data frame
                od_df = pd.DataFrame()

            if len(od_df) > 1:

                self.logger.info(f"Movement extraction was successfully completed for: {orig}-{dest}")

                cleansed_od_df, routeID_stats, portsequence_stats = cleanse_port_sequence(od_df)
                route_rank = (
                    cleansed_od_df
                    .groupby(['IMO', 'route_ID'])[TIME_POSITION]
                    .min()
                    .reset_index()
                    .sort_values(by='TimePosition')
                    .reset_index(drop=True)
                )
                route_rank['unique_route_ID'] = route_rank.index + 1
                cleansed_od_df['week'] = cleansed_od_df['TimePosition'].dt.isocalendar().week
                cleansed_od_df = cleansed_od_df.merge(
                    route_rank[['IMO', 'route_ID', 'unique_route_ID']],
                    how='inner', on=['IMO', 'route_ID']
                )
                cleansed_od_df = cleansed_od_df.merge(
                    routeID_stats[['IMO', 'route_ID', 'journey_time']],
                    how='inner', on=['IMO', 'route_ID']
                )
                cleansed_od_df['elapsed_time'] = cleansed_od_df['journey_time'] - cleansed_od_df['remaining_lead_time']

                filename = os.path.join(self.training_file_output_dir, f"{orig}{dest}.feather")
                routeID_stats_filename = os.path.join(self.output_stats_dir, f"routeID_{orig}{dest}.csv")
                portsequence_stats_filename = os.path.join(self.output_stats_dir, f"portsequence_{orig}{dest}.csv")

                if cleansed_od_df.unique_route_ID.max() >= route_threshold_od:

                    self.logger.info(f"The port sequence cleansing generated training a file for: {orig}-{dest}")
                    self.logger.info(f"The number of cleansed routes for this OD are: {cleansed_od_df.unique_route_ID.max()}")

                    cleansed_od_df.to_feather(filename)
                    routeID_stats.to_csv(routeID_stats_filename, index=False)
                    portsequence_stats.to_csv(portsequence_stats_filename, index=False)
                    self.successful_jobs.append({JOB_NAME: name, JOB_ORIGIN: orig, JOB_DESTINATION: dest})
                    success_odlist.extend([f"{orig}-{dest}"])
                else:
                    self.logger.info(f"The port sequence cleansing resulted in no training file for: {orig}-{dest}")
                    self.failed_jobs.append({JOB_NAME: name, JOB_ORIGIN: orig, JOB_DESTINATION: dest})
                    failed_odlist.extend([f"{orig}-{dest}"])
            else:
                self.logger.info(f"The movement extraction resulted in no training file for: {orig}-{dest}")
                self.failed_jobs.append({JOB_NAME: name, JOB_ORIGIN: orig, JOB_DESTINATION: dest})
                failed_odlist.extend([f"{orig}-{dest}"])

        # Write success/failure ODs to file
        # TODO: This results in re-writing previous success/failure data.
        success_df = pd.DataFrame(success_odlist, columns=['OD'])
        failed_df = pd.DataFrame(failed_odlist, columns=['OD'])

        success_df.to_csv(os.path.join(self.output_root_dir, "ods_successfully_processed.csv"),  index=False)
        failed_df.to_csv(os.path.join(self.output_root_dir, "ods_unsuccessfully_processed.csv"), index=False)

    def get_vessel_od_subframe(self,
                               main_df: Optional[pd.DataFrame],
                               vessel_imo,
                               imo_range_start,
                               orig_port,
                               dest_port,
                               return_slices_only: bool = False,
                               add_lead_times: bool = False,
                               return_journey_starts_only: bool = False
                               ) -> Union[Optional[pd.DataFrame], Optional[Tuple]]:
        """
        main_df must match the earlier construction of IMO_to_digested_port_sequence
        (same index and rows content, but could have new columns)... returns None
        if no matches.

        NOTE: it's not a great convention, but if
        return_journey_starts_only is passed as 2,
        it means return the start and end of each
        journey
        """
        vp = self.imo_to_digested_port_sequence.get(vessel_imo)
        if not vp or len(vp.port_str) == 0:
            return None

        p1 = self.port_to_mapped_port.get(orig_port)
        p2 = self.port_to_mapped_port.get(dest_port)
        od = orig_port + '-' + dest_port  # TODO: make this a method

        if p1 is None or p2 is None or p1 == p2:
            return None

        c1 = vp.port_map.get(p1)
        c2 = vp.port_map.get(p2)
        if c1 is None or c2 is None:
            return None

        # We let the pattern stop with the first occurrence
        # of the destination after the origin.
        pat = re.compile(f"{c1}(?P<intermed>[^{c1}{c2}{JOURNEY_BREAKER_LETTER}]*){c2}")
        matches = list(pat.finditer(vp.port_str))  # NOTE: could also use re.findall()?
        if not matches:  # TODO: add condition (or len(matches) == 0)?
            return None

        # The rows that we select will be a contiguous range in the main DF.
        # We depend on having the main DF sorted by both vessel and time,
        # and the caller tells us where the IMO starts within the
        # rows of the main DF.
        mainslices = []
        odlist = []
        if add_lead_times:
            tcolpos = main_df.columns.get_loc(TIME_POSITION)
            timechunks = []
            jdurs = []

        for m in matches:
            if intermed_port_chars_admissible(m.group('intermed')):
                i1 = imo_range_start + vp.row_pos[m.start()]
                i2 = imo_range_start + vp.row_pos[m.end() - 1]  # inclusive
                if i2 == i1:
                    # options about returning less than the full journey do not apply...
                    # this case should no longer occur because the mapped ports
                    # must be distinct
                    mainslices.append(slice(i1, i1 + 1))
                    if add_lead_times:
                        jdurs.append(0.)
                elif return_journey_starts_only:
                    if return_journey_starts_only == 2:
                        # we can express first and last as an unusual slice,
                        # and the code that is calling this has been adjusted
                        # to call get_slice_len instead of just doing end-start...
                        # might also have worked to use a numpy array instead of
                        # a slice, because of the way the calling code is using
                        # the slice
                        mainslices.append(slice(i1, i2 + 1, (i2 - i1)))
                        odlist.append(od)
                        # and we will still have both the first and last times
                        # available in tchunk (below)
                    else:
                        mainslices.append(slice(i1, i1 + 1))
                        odlist.append(od)
                        if add_lead_times:
                            # can't use the normal case (below) because we will have
                            # only the first time available in tchunk
                            jdurs.append(
                                days_between_ts(main_df.iat[i1, tcolpos],
                                                main_df.iat[i2, tcolpos])
                            )
                else:
                    mainslices.append(slice(i1, i2 + 1))
                    odlist.append(od)
                if add_lead_times:
                    timechunks.append(main_df.iloc[mainslices[-1], tcolpos])
                    if len(jdurs) < len(mainslices):
                        # not a special case that was already handled
                        tchunk = timechunks[-1]
                        jdurs.append(days_between_ts(tchunk.iat[0], tchunk.iat[-1]))

        if not mainslices:
            return None

        elif return_slices_only:
            if add_lead_times:
                return mainslices, timechunks, jdurs, odlist
            else:
                return mainslices, odlist

        else:
            df2: pd.DataFrame
            # Then return a dataframe
            if len(mainslices) == 1:
                df2 = main_df.iloc[mainslices[0]].assign(route_ID=1)
            else:
                df2 = (
                    main_df.iloc[expand_iloc_slice_list(mainslices)]
                    .assign(
                        route_ID=np.repeat(
                            np.arange(1, len(mainslices) + 1),
                            list(map(get_slice_len, mainslices))
                        )
                    )
                )
            if add_lead_times:
                add_lead_time_cols(df2, timechunks, jdurs)
            return df2

    def create_vessel_port_sequence(self,
                                    vessel_df: Optional[pd.DataFrame],
                                    port_col: str = MAPPED_PORT,
                                    journey_breaker_col: Optional[str] = JOURNEY_BREAKER
                                    ) -> VesselPortSequence:
        """
        Computes and returns an instance of VesselPortSequence
        """
        if vessel_df is None:
            vessel_df = self.vessel_movements_df

        if len(vessel_df.index) == 0:
            return VesselPortSequence.EMPTY

        assert vessel_df[TIME_POSITION].is_monotonic_increasing
        assert (vessel_df[IMO] == vessel_df[IMO].iat[0]).all()

        # Reset the index before dropping some rows, so the index values
        # will represent row positions within vessel_df.
        if journey_breaker_col is None:
            ports: pd.Series = vessel_df[port_col].reset_index(drop=True).dropna()
        else:
            # Artificially include the journey breaker positions in the ports
            # string, even if no port was identified; but make those positions
            # contain NaN, which in this code will unambiguously mark a  journey breaker.
            print(f"[VESSEL DF]\n{vessel_df.head()}")
            vdf2 = vessel_df[[port_col, journey_breaker_col]].reset_index(drop=True)
            jbreak = vdf2[journey_breaker_col]
            ports = vdf2[port_col].mask(jbreak, other=np.nan)
            ports = ports[ports.notna() | jbreak]
        if len(ports) == 0:
            return VesselPortSequence.EMPTY
        unique_ports = ports.unique()
        letter_limit = len(PORT_LETTER_CHARS)
        # note: this will now skip a letter if there are any journey breakers
        letter_map = {
            p: PORT_LETTER_CHARS[i]
            for i, p in enumerate(unique_ports) if i < letter_limit and type(p) is str
        }
        port_chars = [
            (
                JOURNEY_BREAKER_LETTER if type(p) is not str
                else letter_map.get(p, '?')
            )
            for p in ports
        ]
        # In the current calling sequence, we require that vessel_df must be
        # a contiguous subrange of the main DF, so we record row positions within
        # vessel_df and we will later offset them to get row positions within
        # the main DF.  For large datasets, it is much faster to deal with row
        # positions rather than index labels.
        return VesselPortSequence(
            port_str=''.join(port_chars),
            port_map=letter_map,
            # the index values of ports are row positions within vessel_df,
            # because we reset the index before taking the notna subset
            row_pos=ports.index.to_numpy()
        )

    def make_imo_range_data(self) -> None:
        """
        Sort movement data. Extract a database describing the start
        index and run length pertaining to each IMO in the dataset.
        Also create a dictionary to lookup these values from the IMO.
        """
        self.logger.info("IMPORTANT! Sorting IMO range data.")
        self.sort_vessel_movements_df()

        self.logger.info("Deriving IMO range data...")

        imo_range_data = np_runlengths(self.vessel_movements_df[IMO])

        self.imo_range_df = pd.DataFrame(dict(
            zip([RANGE_START, RANGE_LENGTH, IMO],
                imo_range_data)
        ))

        self.imo_to_main_range_start = {
            imo: start
            for imo, start in zip(self.imo_range_df[IMO],
                                  self.imo_range_df[RANGE_START])
        }

    def sort_vessel_movements_df(self) -> None:
        """
        The feature extraction process depends on the ordering of rows in
        the vessel movements data. We depend on both levels of this sorting!
        We are also resetting the index values.
        """
        self.vessel_movements_df.sort_values(
            [IMO, 'TimePosition'],
            inplace=True,
            ignore_index=True
        )
        self.is_movement_data_sorted = True

    def compute_stopped_nearest_port_fields(self) -> None:
        """
        Generates a calculated field on the vessel movements data
        (stopped_nearest_port)
        """
        self.logger.info(f"Computing calculated field: {PORT}")
        self.vessel_movements_df[PORT] = (
            self.vessel_movements_df
            .groupby(IMO, group_keys=False)
            .apply(self.closest_port_ser)
        )

        self.logger.info(f"Computing calculated field: {MAPPED_PORT}")
        self.vessel_movements_df[MAPPED_PORT] = (
            self.vessel_movements_df[PORT]
            .map(self.port_to_mapped_port, na_action='ignore')
        )

    def mark_hexes_near_ports(self,
                              resolution: int = 5,
                              rings: int = 2):
        """Returns a map from hex: portlist"""
        self.logger.info("Marking hexes near to ports...")
        hex_ports = defaultdict(list)
        for port, latlon in self.port_to_latlon.items():
            hex1 = h3.geo_to_h3(*latlon, resolution)  # linting error aside, I confirmed this works -ASL
            for hex2 in h3.k_ring(hex1, rings):       # linting error aside, I confirmed this works -ASL
                hex_ports[hex2].append(port)

        self.hex5_to_possible_ports = dict(hex_ports)

    def set_port_latlon_dict(self):
        """Used to look up latitude/longitude of a port based on its code string"""
        self.logger.info("Setting port to latlon dict...")
        self.port_to_latlon = dict(zip(
            self.edited_ports_df['locode'],
            zip(self.edited_ports_df['lat'],
                self.edited_ports_df['lon'])
        ))

    def set_port_to_mapped_port(self):
        """Applies mappings to a port name"""
        self.port_to_mapped_port = dict(zip(
            self.edited_ports_df['locode'],
            self.edited_ports_df['mapped_locode']
        ))

    def load_od_list_df(self, file_path):
        """Reconstitute origin-destination (OD) data (dataframe) from file"""
        self.logger.info("Loading the od list csv file...")
        self.od_list_df = pd.read_csv(file_path)
        self.od_list_df['mapped_origin'] = self.od_list_df['origin'].map(self.port_to_mapped_port)
        self.od_list_df['mapped_destination'] = self.od_list_df['destination'].map(self.port_to_mapped_port)

    def closest_port_ser(self,
                         vessel_df: pd.DataFrame,
                         stopped_only: bool = True):
        """
        Elements with no port within threshold are not included in the
        output series.  Because an indexed Series is returned, the values
        can still be aligned with the original.  Also, it works for
        vessel_df to be already a subset, and then the returned Series
        can be aligned with the original.
        """
        arrived_threshold: int = DISTANCE_FROM_PORT_THRESHOLD_FOR_ARRIVED
        stopped_threshold: float = VESSEL_SPEED_THRESHOLD_FOR_STOPPED

        if stopped_only:
            sub_df = vessel_df[
                vessel_df['NavStatus'].isin(['moored', 'at anchor', 'aground'])
                & (vessel_df['Speed'] < stopped_threshold)
            ]
        else:
            sub_df = vessel_df

        port_set = set()
        for hex1 in sub_df['h3_5'].unique():
            port_set.update(self.hex5_to_possible_ports.get(hex1, []))

        candidate_ports = np.array(list(port_set), dtype=object)
        if len(candidate_ports) > 0 and len(sub_df.index) > 0:
            distances = haversine_vector(
                [self.port_to_latlon[p] for p in candidate_ports],
                list(zip(sub_df['Latitude'], sub_df['Longitude'])),
                Unit.NAUTICAL_MILES,
                comb=True
            )
            # result has one row per movement
            closest = np.argmin(distances, axis=1)
            closest_dist = distances[range(len(closest)), closest]
            full_series = pd.Series(candidate_ports[closest], index=sub_df.index)
            return full_series[closest_dist <= arrived_threshold]
        else:
            return pd.Series(np.nan, index=sub_df.index)

    def load_vessel_movements_dataframe(self, file_path) -> pd.DataFrame:
        """Reconstitute vessel movements data (dataframe) from file"""
        self.logger.info("Loading the vessel movements feather file...")
        return pd.read_feather(file_path)

    def load_ports_file(self, file_path) -> pd.DataFrame:
        """Reconstitute ports data (dataframe) from file"""
        self.logger.info("Loading the ports csv file...")
        return pd.read_csv(file_path)

    @classmethod
    def set_config(cls, user_config_path) -> Dict:
        """Load default configs and apply user overrides"""
        config = cls._load_default_config()

        if user_config_path:
            user_config = cls._load_yaml_file(user_config_path)
            for key, value in user_config.items():
                config[key] = value
        return config

    @classmethod
    def _load_default_config(cls) -> Dict:
        """Load default configurations from YAML file"""
        config_file_name = CONFIG_FILE_DEFAULT_FILENAME
        config_file_path = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            config_file_name
        )
        return cls._load_yaml_file(config_file_path)

    @staticmethod
    def load_default_configs():
        return OriginDestinationRouteExtractor._load_default_config()

    @staticmethod
    def _load_yaml_file(file_path) -> Dict:
        """Load yaml file into a python dictionary"""
        with open(file_path, 'r') as yaml_file:
            yaml_data = yaml.load(yaml_file, Loader=yaml.FullLoader)
            return yaml_data
