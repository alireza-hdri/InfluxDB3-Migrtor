import os
import shutil
import subprocess
import configparser
from itertools import islice


class InfluxDB3Migrator:
    def __init__(self, config_path):
        
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        main = self.config["main"]
        self.measurements_dir = main.get("measurements_dir")

        influxdb_old = self.config["influxdb_old"]
        self.old_influxdb_container_name = influxdb_old.get("old_influxdb_container_name")
        self.old_influxdb_database_name = influxdb_old.get("old_influxdb_database_name")


        influxdb3 = self.config["influxdb3"]
        self.influxdb3_container_name = influxdb3.get("influxdb3_container_name")
        self.influxdb3_database_name = influxdb3.get("influxdb3_database_name")
        self.admin_token = influxdb3.get("admin_token")
        self.mountpoint_in_host = influxdb3.get("mountpoint_in_host")
        self.mountpoint_in_container = influxdb3.get("mountpoint_in_container")

        
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
    
    def chunker(self, measurement, lines_per_file=2000 ):
        output_dir = f"./{self.measurements_dir}/measurements_chunks/{measurement}"
        os.makedirs(output_dir, exist_ok=True)
        base_path = os.path.join(output_dir, f"{measurement}_part")

        file_index = 1
        with open(f"./{self.measurements_dir}/measurements/{measurement}.lp", "r") as input_file:
            lines = input_file.readlines()
            for i in range(0, len(lines), lines_per_file):
                output_path = f"{base_path}_{file_index}.lp"
                with open(output_path, "w") as output_file:
                    output_file.writelines(lines[i:i+lines_per_file])
                file_index += 1
    def change_to_influxdb3_line_protocol(self,measurement,lines_per_file=2000):

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
        
        self.chunker(measurement, lines_per_file)
    
    def cleaner(self,measurement ):
        if os.path.exists(f"{self.mountpoint_in_host}/{self.measurements_dir}/measurements_chunks/{measurement}"):
            shutil.rmtree(f"{self.mountpoint_in_host}/{self.measurements_dir}")
        if os.path.exists(f"{self.mountpoint_in_host}/{self.measurements_dir}/measurements"):
            shutil.rmtree(f"{self.mountpoint_in_host}/{self.measurements_dir}/measurements")

    def migrator(self):
        measurements_list = self.collect_measurements_list()

        for measurement in measurements_list:
            self.change_to_influxdb3_line_protocol(measurement)
            self.chunker(measurement)
            print(f"Measurements {measurement} is migrating to influxdb3 line protocol.")
            chumked_measurements_list = os.listdir(f"./{self.measurements_dir}/measurements_chunks/{measurement}")
            for chunked_measurement in chumked_measurements_list:
                chunked_measurement_path = f"./{self.measurements_dir}/measurements_chunks/{measurement}/{chunked_measurement}"
                command = (
                    f"docker exec -it {self.influxdb3_container_name} "
                    f"influxdb3 write --database '{self.influxdb3_database_name}' --token {self.admin_token} --file {chunked_measurement_path}"
                )
                try:
                    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

                except subprocess.CalledProcessError as e:
                    print("Error migrating measurements:", e.stderr)

            print(f"Measurements {measurement} is migrated to influxdb3 line protocol.")
            print("Start Cleaning For Next Measurement")
            self.cleaner(measurement)

if __name__ == "__main__":
    config_path="./config.ini"
    influxdb_migrator = InfluxDB3Migrator(config_path)
    influxdb_migrator.migrator()    

