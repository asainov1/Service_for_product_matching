# 🛍️ Service for Product Matching

A Python-based microservice for product matching across e-commerce platforms.

## 🚀 Features

- Ingest product data from multiple sources (CSV, API, etc.)
- Run multiple matching algorithms
- Monitor service performance and accuracy
- Modular and easy to extend

## 🗂️ Project Structure

```
.
├── data_inflow.py         # Data ingestion logic
├── data_inflow_wb.py      # Specialized data inflow for Wildberries
├── cv_service.py          # CV / NLP based matching logic
├── main_algo.py           # Main matching algorithm
├── sub_algo.py            # Supporting matching algorithms
├── monitoring.py          # Service monitoring and logging
├── monitoring_wb.py       # Monitoring for Wildberries specific
├── temp.py                # Temporary script or tests
```

## 🛠️ Installation

```bash
pip install -r requirements.txt
```

## 🧑‍💻 Usage Example

```python
from main_algo import run_matching

result = run_matching("input_data.csv")
print(result)
```

## 📊 Monitoring

Run monitoring with:
```bash
python monitoring.py
```

## ✅ Requirements

- Python 3.8+
- pandas
- numpy
- scikit-learn

## 📝 License

MIT License

## 🙌 Contribution

Feel free to fork this repository, submit issues or pull requests.
