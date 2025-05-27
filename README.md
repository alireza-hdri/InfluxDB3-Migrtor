# InfluDB3-Migrtor

This Python-based script allows for the migration of data from InfluxDB 1.8 to InfluxDB 3.x. The migration process assumes that both the old and new InfluxDB instances are running in Docker containers.

## Prerequisites

Before running the migration script, ensure the following:

- **Docker** is installed and running on your system.
- Both **InfluxDB 1.8** and **InfluxDB 3.x** should be running inside Docker containers.
- The script is written in **Python** and requires the `config.ini` configuration file to be completed prior to execution.

## Configuration (config.ini)

Before running the migration script, configure the config.ini file. The config.ini file contains necessary information for connecting to both InfluxDB instances.

## Migration Script

The migration is performed using the Python script migrator.py. This script fetches data from the InfluxDB 1.8 instance and inserts it into the InfluxDB 3.x instance.

To execute the migration:

- Ensure the configuration file (config.ini) is completed.

- Run the migration script

The script will connect to both InfluxDB instances, extract data from the old InfluxDB 1.8, and migrate it to the new InfluxDB 3.x instance.

## Script Overview

### `migrator.py`

- This Python script connects to both the InfluxDB 1.8 and InfluxDB 3.x instances.
- It reads the configuration from `config.ini`.
- The script fetches the data from the specified measurement in InfluxDB 1.8.
- It processes the data and inserts it into the InfluxDB 3.x instance in the appropriate bucket.

### `config.ini`

- This file holds all necessary configuration for both InfluxDB instances. Ensure the correct settings for both the old and new InfluxDB instances are provided.

## Notes

- Ensure that both Docker containers (InfluxDB 1.8 and InfluxDB 3.x) are running and accessible.
- InfluxDB 1.8 uses **databases**, while InfluxDB 3.x uses **buckets**. Make sure the `bucket` in the InfluxDB 3.x section of `config.ini` is properly set.
- The script does not handle schema transformations. It migrates data as-is, but you may need to adjust your InfluxDB schema as needed.

## Troubleshooting

- If you encounter connection issues, verify that both InfluxDB containers are running and that the ports are open.
- If the migration script fails to run, check the `config.ini` file for incorrect settings or missing fields.

## Contributing

Feel free to contribute to this project! If you have suggestions, improvements, or bug fixes, please fork the repository and submit a pull request. All contributions are welcome.

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and commit them.
4. Push your changes to your forked repository.
5. Submit a pull request describing your changes.

## License

This project is licensed under the **Apache 2.0 License** - see the [LICENSE](LICENSE) file for details.
## Acknowledgments

- Special thanks to the [InfluxDB](https://www.influxdata.com/) team for providing open-source tools to manage time series data.
- Docker for containerizing the InfluxDB instances for seamless migration.
