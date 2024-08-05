import psycopg2
import os
from collections import defaultdict
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '6875')
DB_USER = os.getenv('DB_USER', 'materialize')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_NAME = os.getenv('DB_NAME', 'materialize')

THRESHOLD_UPPER = int(os.getenv('THRESHOLD_UPPER', '180'))  # Scale up if utilization exceeds this percent
THRESHOLD_LOWER = int(os.getenv('THRESHOLD_LOWER', '40'))   # Scale down if utilization falls below this percent

# Define cluster sizes in ascending order
CLUSTER_SIZES = [
    '25cc', '50cc', '100cc', '200cc', '300cc', '400cc', 
    '600cc', '800cc', '1200cc', '1600cc', '3200cc', '6400cc', 
    '128C', '256C', '512C'
]

def get_cluster_utilization():
    try:
        # Connect to the Materialize database
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        cursor = connection.cursor()

        # Execute the query
        query = """
        SELECT
            c.name AS cluster_name,
            r.size AS current_size,
            round(sum(m.memory_bytes + m.disk_bytes) / sum(s.memory_bytes) * 100) AS memory_utilization_pct
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
        """
        cursor.execute(query)
        results = cursor.fetchall()

        return results
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to Materialize: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def scale_cluster(cluster_name, current_size, action):
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        connection.autocommit = True  # Disable transaction block
        cursor = connection.cursor()

        current_index = CLUSTER_SIZES.index(current_size)
        if action == 'scale up' and current_index < len(CLUSTER_SIZES) - 1:
            new_size = CLUSTER_SIZES[current_index + 1]
        elif action == 'scale down' and current_index > 0:
            new_size = CLUSTER_SIZES[current_index - 1]
        else:
            print(f"No scaling action required for cluster {cluster_name} with size {current_size}.")
            return

        # Execute the ALTER CLUSTER command
        alter_query = f"ALTER CLUSTER {cluster_name} SET (size = '{new_size}');"
        cursor.execute(alter_query)
        print(f"Cluster {cluster_name} scaled {action} to size {new_size}.")

    except (Exception, psycopg2.Error) as error:
        print(f"Error while scaling cluster {cluster_name}: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def main():
    results = get_cluster_utilization()

    cluster_info = defaultdict(lambda: {"sizes": set(), "utilization": 0})

    for row in results:
        cluster_name, current_size, memory_utilization_pct = row
        cluster_info[cluster_name]["sizes"].add(current_size)
        cluster_info[cluster_name]["utilization"] = memory_utilization_pct

    for cluster_name, info in cluster_info.items():
        if len(info["sizes"]) != 1:
            print(f"Error: Cluster {cluster_name} has replicas with different sizes: {info['sizes']}")
            continue

        current_size = next(iter(info["sizes"]))
        if info["utilization"] > THRESHOLD_UPPER:
            scale_cluster(cluster_name, current_size, 'scale up')
        elif info["utilization"] < THRESHOLD_LOWER:
            scale_cluster(cluster_name, current_size, 'scale down')
        else:
            print(f"Cluster {cluster_name} utilization is within normal range ({info['utilization']}%)")

if __name__ == "__main__":
    main()
