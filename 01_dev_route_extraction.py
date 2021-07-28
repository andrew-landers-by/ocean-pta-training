import logging
import os.path
from ocean_pta_training import OriginDestinationRouteExtractor


def main():
    logger = logging.getLogger(__name__)
    logger.info("BEGINNING AN INCREMENTAL DATA EXTRACTION PROCESS FOR OCEAN PTA")

    try:
        config = OriginDestinationRouteExtractor.load_default_configs()
        data_dir = config.get("LOCAL_MATERIAL_ROOT_DIR")

        feature_extractor = OriginDestinationRouteExtractor(
            path_to_ports_file=os.path.join(data_dir, "ports_trimmed_modified.csv"),
            path_to_vessel_movements_data=os.path.join(data_dir, "vessel_movements_with_hexes.feather"),
            path_to_od_file=os.path.join(data_dir, "od_routes_v2.csv"),
            config_path=None  # use default configs for dev runs
        )
        feature_extractor.run()
    except Exception as e:
        logger.exception(f"Error: {e}")


if __name__ == "__main__":
    main()
