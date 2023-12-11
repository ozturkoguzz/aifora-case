# Spark Duplicate Checker

This Python application uses Apache Spark to identify and analyze duplicate records in a given dataset. The code is organized into two files: `spark_duplicate_checker.py` containing the main functionality, and `test_duplicate_checker.py` as an example of how to use the SparkDuplicateChecker class.

## Prerequisites

Before running the code, ensure that your system meets the following requirements:

1. **Update Repository List:**
    ```bash
    sudo apt update
    ```

2. **Python 3:**
    Verify if Python 3 is installed:
    ```bash
    python3 --version
    ```
    If not installed, run:
    ```bash
    sudo apt install python3
    ```

3. **pip3:**
    Verify if pip3 is installed:
    ```bash
    pip3 --version
    ```
    If not installed, run:
    ```bash
    sudo apt install python3-pip
    ```

4. **Java 11:**
    Verify if Java is installed:
    ```bash
    java --version
    ```
    If not installed, run:
    ```bash
    sudo apt install openjdk-11-jdk
    ```

## Installation

To install the required Python libraries, run the following command:

```bash
pip install -r requirements.txt
```

## Usage

1. **Clone the Repository:**
    ```bash
    git clone git@github.com:ozturkoguzz/aifora-case.git
    cd aifora-case
    ```

2. **Run the Example:**
    Execute the test script to see the duplicate checking in action:
    ```bash
    python test_duplicate_checker.py
    ```

3. **Customize Configuration:**
    Adjust the `config.yml` file to configure Spark settings if needed.

4. **Integrate with Your Data:**
    Modify `test_duplicate_checker.py` to use your own dataset and columns for duplicate checking.

## Code Structure

- **`spark_duplicate_checker.py`:**
    - Main module containing the SparkDuplicateChecker class with methods for duplicate checking.
    - Handles Spark session creation, configuration loading, and duplicate checking.

- **`test_duplicate_checker.py`:**
    - Example script demonstrating how to use SparkDuplicateChecker with a sample dataset.

- **`config.yml`:**
    - Configuration file specifying Spark settings such as the app name and master.

- **`requirements.txt`:**
    - List of Python libraries required for the project.

## Customization

- **Configuration:**
    - Adjust Spark configurations in `config.yml` according to your requirements.

- **Data and Columns:**
    - Modify the `data` and `columns_to_check` variables in `test_duplicate_checker.py` with your dataset and columns.

- **Error Handling:**
    - The code includes basic error handling, but you may customize it based on your needs.

## License

This project is licensed under the Apache License.