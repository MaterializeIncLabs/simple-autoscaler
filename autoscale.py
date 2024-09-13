import asyncio
import logging
import os
from collections import defaultdict
from types import NoneType
from typing import Literal as StringLiteral
from typing import TypedDict, cast

import psycopg
from dotenv import load_dotenv
from psycopg.sql import SQL, Identifier, Literal

logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger("")

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
LOGGER.handlers = []
LOGGER.addHandler(handler)

# Load environment variables from .env file
load_dotenv()

# Configuration variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "6875")
DB_USER = os.getenv("DB_USER", "materialize")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "materialize")

THRESHOLD_UPPER = int(
    os.getenv("THRESHOLD_UPPER", "180")
)  # Scale up if utilization exceeds this percent
THRESHOLD_LOWER = int(
    os.getenv("THRESHOLD_LOWER", "20")
)  # Scale down if utilization falls below this percent

# Define cluster sizes in ascending order
CLUSTER_SIZES = [
    "25cc",
    "50cc",
    "100cc",
    "200cc",
    "300cc",
    "400cc",
    "600cc",
    "800cc",
    "1200cc",
    "1600cc",
    "3200cc",
    "6400cc",
    "128C",
    "256C",
    "512C",
]


class AlterSettings(TypedDict):
    graceful: bool
    max_size: str
    min_size: str
    timeout: str
    on_timeout_action: str


class ScaleSettings(TypedDict):
    upper_threshold: float
    lower_threshold: float


async def get_cluster_utilization(
    connection: psycopg.AsyncConnection,
) -> list[tuple[str, str, int]]:
    # Connect to the Materialize database
    async with connection.cursor() as cursor:
        query = SQL("""
        SELECT
            c.name AS cluster_name,
            r.size AS current_size,
            round(sum(m.memory_bytes + m.disk_bytes) / sum(s.memory_bytes) * 100,2) AS memory_utilization_pct
        FROM mz_internal.mz_cluster_replica_metrics AS m
        INNER JOIN mz_cluster_replicas AS r
            ON (m.replica_id = r.id)
        INNER JOIN mz_cluster_replica_sizes AS s
            ON (r.size = s.size)
        INNER JOIN mz_clusters AS c
            ON (r.cluster_id = c.id)
        WHERE m.replica_id LIKE 'u%'
        GROUP BY c.name, r.size
        ORDER BY memory_utilization_pct DESC;
        """)
        try:
            await cursor.execute(query)
            results = await cursor.fetchall()
            return cast(list[tuple[str, str, int]], results)
        except (Exception, psycopg.Error) as error:
            LOGGER.error(f"Failed to connect to Materialize: {error}")
        return []


async def check_for_pending_updates(connection) -> list[str]:
    async with connection.cursor() as cursor:
        query = SQL("""
        SELECT c.name from mz_internal.mz_pending_cluster_replicas as p
        LEFT JOIN mz_catalog.mz_cluster_replicas as r ON (p.id = r.id)
        LEFT JOIN mz_catalog.mz_clusters as c ON (r.cluster_id = c.id);
        """)
        try:
            await cursor.execute(query)
            results = await cursor.fetchall()
            cast(list[list[str]], results)
            # flatten
            return [column for row in results for column in row]
        except (Exception, psycopg.Error) as error:
            LOGGER.error(f"Failed to connect to Materialize: {error}")
        return []


async def scale_cluster(
    cluster_name: str, current_size: str, action: str, alter_settings: AlterSettings
):
    min_size_idx = CLUSTER_SIZES.index(alter_settings["min_size"])
    max_size_idx = CLUSTER_SIZES.index(alter_settings["max_size"])
    async with await mz_connect() as connection:
        async with connection.cursor() as cursor:
            current_index = CLUSTER_SIZES.index(current_size)
            # Check size bounds
            if action == "scale up":
                if current_index >= max_size_idx:
                    LOGGER.warning(
                        f"WARNING: failed {action}: cluster {cluster_name} is already at greatest size {current_size}."
                    )
                    return
                new_size = CLUSTER_SIZES[current_index + 1]
            elif action == "scale down":
                if current_index <= min_size_idx:
                    LOGGER.debug(
                        f"cluster {cluster_name} is already at smallest size {current_size}."
                    )
                    return
                new_size = CLUSTER_SIZES[current_index - 1]
            else:
                LOGGER.debug(
                    f"No scaling action required for cluster {cluster_name} with size {current_size}."
                )
                return

            # Execute the ALTER CLUSTER command
            alter_query = SQL(
                "ALTER CLUSTER {cluster_name} SET (size = {new_size});"
            ).format(
                cluster_name=Identifier(cluster_name),
                new_size=Literal(new_size),
            )
            if alter_settings["graceful"]:
                alter_query = SQL(
                    "ALTER CLUSTER {cluster_name} SET (size = {new_size})  WITH (WAIT UNTIL READY (TIMEOUT = {timeout}, ON TIMEOUT = {on_timeout}));"
                ).format(
                    cluster_name=Identifier(cluster_name),
                    new_size=Literal(new_size),
                    timeout=Literal(alter_settings["timeout"]),
                    on_timeout=Literal(alter_settings["on_timeout_action"]),
                )

            LOGGER.info(
                f"""
                Scaling the cluster {cluster_name} with: 
                {alter_query}
                """
            )
            try:
                await cursor.execute(alter_query)
            except (Exception, psycopg.Error) as error:
                LOGGER.error(f"Failed scaling cluster {cluster_name}: {error}")
            LOGGER.info(f"Cluster {cluster_name} scaled {action} to size {new_size}.")


class ClusterInfo(TypedDict):
    # name: str
    sizes: set[str]
    utilization: float


def default_cluster_info() -> ClusterInfo:
    return {
        "sizes": set(),
        "utilization": 0,
    }


async def scale(
    cluster_filter: list[str],
    connection: psycopg.AsyncConnection,
    alter_settings: AlterSettings,
    scale_settings: ScaleSettings,
    wait_for_actions: bool,
):
    results = await get_cluster_utilization(connection)

    pending_clusters = await check_for_pending_updates(connection)
    LOGGER.info(
        f"Clusters {pending_clusters} have pending alterations and will be skipped"
    )

    clusters = defaultdict(default_cluster_info)

    for row in results:
        cluster_name, current_size, memory_utilization_pct = row
        if cluster_filter and cluster_name in cluster_filter:
            continue
        if cluster_name in pending_clusters:
            continue
        if not memory_utilization_pct or isinstance(memory_utilization_pct, NoneType):
            LOGGER.info(
                f"Failed to get utilizization for {cluster_name}: cluster may have just started"
            )
            continue
        clusters[cluster_name]["sizes"].add(current_size)
        clusters[cluster_name]["utilization"] = memory_utilization_pct

    actions = []
    for cluster_name, info in clusters.items():
        LOGGER.debug(f"Evaluating cluster: {cluster_name}")
        if cluster_name in pending_clusters:
            LOGGER.info(
                f"Cluster {cluster_name} is undergoing an alter and will be skipped"
            )
            continue
        cast(str, cluster_name)
        if len(info["sizes"]) != 1:
            LOGGER.info(
                f"Too many cluster sizes, Cluster {cluster_name} may be resizing: {info['sizes']}"
            )
            continue

        current_size = next(iter(info["sizes"]))
        utilization = info["utilization"]

        scale_action = "None"
        if utilization > scale_settings["upper_threshold"]:
            scale_action = "scale up"
            LOGGER.info(
                f"Cluster {cluster_name} mem utilization {utilization}% is above threshold {THRESHOLD_UPPER}%. Setting action {scale_action}"
            )
        elif utilization < scale_settings["lower_threshold"]:
            scale_action = "scale down"
            LOGGER.info(
                f"Cluster {cluster_name} mem utilization {utilization}% is below threshold {THRESHOLD_LOWER}%. Setting action {scale_action}"
            )
        else:
            LOGGER.info(
                f"Cluster {cluster_name} utilization is within normal range ({utilization}%)"
            )
        if scale_action != "None":
            actions.append(
                [
                    cluster_name,
                    current_size,
                    scale_action,
                ]
            )
    LOGGER.debug(f"Starting scale events {actions}")
    # give tasks a chance to kick off
    tasks = []
    for action in actions:
        tasks.append(
            asyncio.create_task(
                scale_cluster(*action, alter_settings=alter_settings),
                name=f"scale_event_{action[0]}_{action[2]}",
            )
        )
    if wait_for_actions:
        await asyncio.gather(*tasks)
    else:
        await asyncio.sleep(0.1)


async def mz_connect() -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(
        f"host={DB_HOST} port={DB_PORT} user={DB_USER} password={DB_PASSWORD} dbname={DB_NAME} sslmode=require",
        autocommit=True,
    )


async def main(
    # Execute a set of checks /scale actions
    continuous: bool = False,
    # The period to check and execute scale actions
    poll_period_seconds: int = 10,
    # The clustes to check and scale, comma separated
    clusters: list[str] = [],
    # Upper range of normal MEM percentage
    upper_threshold_percent: float = THRESHOLD_UPPER,
    # Lower range of normal MEM percentage
    lower_threshold_percent: float = THRESHOLD_LOWER,
    # Whether to not use graceful resizing
    graceful_resize: bool = False,
    # graceful resize timeout
    graceful_alter_timeout: str = "10m",
    # graceful resize on timeout action
    graceful_timeout_action: StringLiteral["COMMIT", "ROLLBACK"] = "COMMIT",
    # Maximum cluster size to use (must be a valid cluster size)
    max_size: str = "400cc",
    # Minimum cluster size to use (must be a valid cluster size)
    min_size: str = "25cc",
):
    """
    Auto Scaler for materialize \n
    This will inspect the current memory usage of all clusters in a materialize environment
    to determine if the cluster should scale up or down. This determination can be configured
    by setting an upper or lower threshold value, as can the clusters considered for scaling. \n

    :param continuous: Executes a single set of checks/scale actions then returns.
    :param poll_period_seconds:  The period to check and execute scale actions.
    :param clusters: A comma separated list of clusters to target.
    :param upper_threshold:  Upper range of normal MEM percentage.
    :param lower_threshold_percent Lower range of normal MEM percentage.
    :param no_graceful_resize:  Whether to not use graceful resizing.
    :param graceful_alter_timeout: graceful resize timeout.
    :param graceful_timeout_action: graceful resize on timeout action.
    :param max_size: Maximum cluster size to use (must be a valid cluster size).
    :param min_size: Minimum cluster size to use (must be a valid cluster size).
    :returns: None
    """
    LOGGER.info(f"setting up autoscaler for {clusters}")
    alter_settings: AlterSettings = {
        "graceful": graceful_resize,
        "max_size": max_size,
        "min_size": min_size,
        "timeout": graceful_alter_timeout,
        "on_timeout_action": graceful_timeout_action,
    }
    scale_settings: ScaleSettings = {
        "upper_threshold": upper_threshold_percent,
        "lower_threshold": lower_threshold_percent,
    }

    async with await mz_connect() as base_connection:
        if not continuous:
            await scale(
                clusters,
                base_connection,
                alter_settings,
                scale_settings,
                wait_for_actions=True,
            )
        else:
            while True:
                await scale(
                    clusters,
                    base_connection,
                    alter_settings,
                    scale_settings,
                    wait_for_actions=False,
                )
                await asyncio.sleep(poll_period_seconds)


if __name__ == "__main__":
    import fire

    fire.Fire(main)
