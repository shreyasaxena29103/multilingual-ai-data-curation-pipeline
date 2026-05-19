# multilingual-ai-data-curation-pipeline

Real-world multilingual AI datasets often contain:
- noisy text
- duplicate samples
- inconsistent formatting
- low-quality records

These issues negatively impact downstream model training quality.

Built a distributed PySpark pipeline on Databricks to:
- stream multilingual datasets
- clean and normalize text
- score data quality
- remove duplicate records
- generate curated Delta datasets

Features:
✔ Distributed Spark processing  
✔ Text normalization  
✔ Quality filtering  
✔ Duplicate removal  
✔ Delta Lake storage
