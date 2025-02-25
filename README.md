
### **PySpark AG News Word Count Processor**

This project processes the **AG News dataset** using **PySpark** to perform word count analysis. It includes functionalities to:  
- **Load and preprocess news data:** Read raw AG News data and apply basic preprocessing.  
- **Clean text:** Remove punctuation, convert to lowercase, and standardize text for analysis.  
- **Count words:** Tally occurrences of specific target words or all unique words.  
- **Store results:** Save the output in **Parquet** format for efficient storage and retrieval.

### **Tech Stack & Features**  
- ✅ **Apache Spark** for distributed data processing  
- ✅ **Docker** for containerized execution, ensuring consistency across environments  
- ✅ **GitHub Actions** for automated testing, linting, and deployment  
- ✅ **YAML-based configuration** for easy customization

### **Usage**

#### **Using the Python CLI**

You can run the processor directly using Python. Here are two available commands:

- **Process a subset of data:**  
  ```bash
  python run.py process_data --cfg ../config/cfg.yaml --dataset news --dirout ../../output
  ```

- **Process all data:**  
  ```bash
  python run.py process_data_all --cfg ../config/cfg.yaml --dataset news --dirout ../../output
  ```

> **Note:** Adjust the relative paths in the configuration (`--cfg`), dataset (`--dataset`), and output (`--dirout`) options as needed based on your project structure.

---

#### **Using Docker**

You can also run the processor inside a Docker container. The example below mounts a local directory (e.g., `ztmp`) to the container to persist output data:

```bash
docker run -v "${PWD}\ztmp:/app/ztmp" agnews python code/src/run.py process_data_all --cfg code/config/cfg.yaml --dataset news --dirout ztmp/data
```

> **Notes:**
> - Ensure that the volume mapping (`-v "${PWD}\ztmp:/app/ztmp"`) correctly reflects your local and container directory structure.  
> - Adjust the command paths if your project layout differs.

