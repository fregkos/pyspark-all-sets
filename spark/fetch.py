import requests
import json
import time
import sys


def get_shuffle_metrics():
    """Get shuffle metrics from the Spark UI API"""
    try:
        # Get application ID
        apps_response = requests.get("http://localhost:4040/api/v1/applications")
        apps = apps_response.json()

        if not apps:
            print("No active Spark application found")
            return

        app_id = apps[0]["id"]

        # Get stages
        stages_response = requests.get(
            f"http://localhost:4040/api/v1/applications/{app_id}/stages"
        )
        stages = stages_response.json()

        total_shuffle_read = 0
        total_shuffle_write = 0

        # Process each stage
        for stage in stages:
            # Skip if no shuffle metrics
            if "shuffleReadMetrics" not in stage:
                continue

            # Extract shuffle metrics
            shuffle_read = stage["shuffleReadMetrics"].get("totalBytesRead", 0)
            shuffle_write = stage.get("shuffleWriteMetrics", {}).get("bytesWritten", 0)

            total_shuffle_read += shuffle_read
            total_shuffle_write += shuffle_write

            print(f"Stage {stage['stageId']}:")
            print(f"  Shuffle Read: {shuffle_read} bytes")
            print(f"  Shuffle Write: {shuffle_write} bytes")

        print("\nTotal Shuffle Metrics:")
        print(
            f"  Total Shuffle Read: {total_shuffle_read} bytes ({total_shuffle_read / (1024*1024):.2f} MB)"
        )
        print(
            f"  Total Shuffle Write: {total_shuffle_write} bytes ({total_shuffle_write / (1024*1024):.2f} MB)"
        )

    except Exception as e:
        print(f"Error getting metrics: {e}")


if __name__ == "__main__":
    print("Spark Shuffle Metrics Monitor")
    print("Run this script while your Spark job is running")
    print("Press Ctrl+C to exit")

    try:
        while True:
            print("\n=== Shuffle Metrics ===")
            get_shuffle_metrics()
            print("======================")
            time.sleep(5)  # Check every 5 seconds
    except KeyboardInterrupt:
        print("\nExiting...")
