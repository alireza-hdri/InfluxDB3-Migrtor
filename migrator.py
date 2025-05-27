import os
import subprocess
import csv
from itertools import islice

old_influxdb_container_name = "asiatech-influxdb"
old_influxdb_database_name = "opentsdb"
mountpoint_in_container = "/backup"
mountpoint_in_host = "."
measurements_dir = "measurements_dir"
class InfluxDB3Migrator:
    def __init__(self, old_influxdb_container_name, old_influxdb_database_name, mountpoint_in_container, mountpoint_in_host,measurements_dir):
        self.old_influxdb_container_name = old_influxdb_container_name
        self.old_influxdb_database_name = old_influxdb_database_name
        self.mountpoint_in_container = mountpoint_in_container
        self.mountpoint_in_host = mountpoint_in_host
        self.measurements_dir = measurements_dir
        
    def collect_measurements_list(self):
        list_measurements_command = (
                f"docker exec -it {self.old_influxdb_container_name} "
                f"influx -database '{self.old_influxdb_database_name}' -execute 'show MEASUREMENTS'"
        )

        try:
            result = subprocess.run(list_measurements_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            measurements = result.stdout.strip().split("\n")
            measurements = [line.strip() for line in measurements if line.strip()]
            measurements_list = []
            for measurement in measurements:
                measurements_list.append(measurement)
                measurements_count = len(measurements_list)
            print(f"Total number of measurements in collect_measurements_list function: {measurements_count}")  # Print the total number of measurements (measurements_count)
            return measurements_list
        
        except subprocess.CalledProcessError as e:
            print("Error collecting measurements:", e.stderr)

    def collect_measurements_data(self):

        measurements_list = self.collect_measurements_list()

        # Ensure that local path exists
        if not os.path.exists(f"{self.mountpoint_in_host}/{self.measurements_dir}"):
            os.makedirs(f"{self.mountpoint_in_host}/{self.measurements_dir}")
        
        for measurement in measurements_list:
            command = (
                f"docker exec -it {self.old_influxdb_container_name} "
                f"influx -database '{self.old_influxdb_database_name}' -execute 'select * from \"{measurement}\"' > {self.mountpoint_in_host}/{self.measurements_dir}/{measurement}.txt"
            )

            try:
                result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

            except subprocess.CalledProcessError as e:
                print("Error collecting measurements:", e.stderr)

    def create_collect_measurements_data(self, measurement):

        if not os.path.exists(f"./{self.measurements_dir}"):
            os.makedirs(f"./{self.measurements_dir}")
            os.makedirs(f"./{self.measurements_dir}/scripts")
            os.makedirs(f"./{self.measurements_dir}/measurements")

        with open (f"./{self.measurements_dir}/scripts/measurements_list_creator.txt", "w") as f:
            f.write(f"docker exec -it {self.old_influxdb_container_name} influx -database '{self.old_influxdb_database_name}' -execute 'select * from \"{measurement}\"' > ./{self.measurements_dir}/measurements/{measurement}.txt\n")
        f.close
  
        command = (
            # Create Measurements Data
            f"bash ./{self.measurements_dir}/scripts/measurements_list_creator.txt"
        )
        try:
            result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        except subprocess.CalledProcessError as e:
            print("Error collecting measurements:", e.stderr)
    
    def change_to_influxdb3_line_protocol(self,measurement):
        self.create_collect_measurements_data(measurement)

        input_path = f"./{self.measurements_dir}/measurements/{measurement}.txt"
        output_path = f"./{self.measurements_dir}/measurements/{measurement}.lp"

        with open(input_path, "r") as f, open(output_path, "w") as p:
            # Skip first 3 lines, assuming header
            for line in islice(f, 3, None):
                line = line.strip()
                if not line:
                    continue

                parts = line.split()
                if len(parts) < 3:
                    print(f"Skipping invalid line: {line}")
                    continue

                timestamp = parts[0]
                host = parts[1]
                value = parts[2]

                p.write(f"{measurement},host={host} value={value} {timestamp}\n")

if __name__ == "__main__":
    influxdb_migrator = InfluxDB3Migrator(old_influxdb_container_name, old_influxdb_database_name, mountpoint_in_container, mountpoint_in_host,measurements_dir)
    influxdb_migrator.change_to_influxdb3_line_protocol("netdata.anomaly_detection.type_anomaly_rate_on_asiatech_controller.disk_mops")    

