simple-autoscaler
==================
A very naive attempt at an autoscale script built on top of the cluster utilization logic we've been developing.

## Setup
To setup use Python 3.11 or greater and run `pip install -r requirements.txt`

## Running
Basic:
`python autoscale.py`

There are two ways to run this, 'continuous' or 'one-shot'.
- In continuous mode this this script will periodically poll the metrics for all
clusters and making scaling when the memory usage is beyond defined thresholds.
- In one-shot mode, only one check/scale cycle will complete and the script will
exit.

This script is now compatible with private preview feature [graceful resizing](https://materialize.com/docs/sql/alter-cluster/#graceful-cluster-resizing) which will allow the scaling events
to occur with no noticeable downtime. *This is currently feature gated, and cannot be used on clusters with sinks or sources.*


Currently all thresholds and config options apply to all clusters. If you want to run this with different settings for different clusters you should
just run the script multiple times specifying the `--clusters` you want and the thresholds, config, and max/mins size applicable to the specified cluster.
If you want to auto-scale a cluster with a source or sink you can not use graceful resizing.

```
NAME
    autoscale.py - Auto Scaler for materialize

SYNOPSIS
    autoscale.py <flags>

DESCRIPTION
    This will inspect the current memory usage of all clusters in a materialize environment
    to determine if the cluster should scale up or down. This determination can be configured
    by setting an upper or lower threshold value, as can the clusters considered for scaling.

FLAGS
    --continuous=CONTINUOUS
        Type: bool
        Default: False
    -p, --poll_period_seconds=POLL_PERIOD_SECONDS
        Type: int
        Default: 10
        The period to check and execute scale actions.
    --clusterss=CLUSTERS
        Type: list
        Default: []
        A comma separated list of clusters to target.
    -u, --upper_threshold_percent=UPPER_THRESHOLD_PERCENT
        Type: float
        Default: 20
    -l, --lower_threshold_percent=LOWER_THRESHOLD_PERCENT
        Type: float
        Default: 120
    --graceful_resize=GRACEFUL_RESIZE
        Type: bool
        Default: True
    --graceful_alter_timeout=GRACEFUL_ALTER_TIMEOUT
        Type: str
        Default: '10m'
    --graceful_timeout_action=GRACEFUL_TIMEOUT_ACTION
        Type: Literal
        Default: 'COMMIT'
    --max_size=MAX_SIZE
        Type: str
        Default: '400cc'
        Maximum cluster size to use (must be a valid cluster size)
    --min_size=MIN_SIZE
        Type: str
        Default: '25cc'
        Minimum cluster size to use (must be a valid cluster size)

```
