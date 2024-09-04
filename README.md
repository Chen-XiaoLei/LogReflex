# LogReflex

This is the code for paper Log Parsing with LLMs Featuring Self-Reflection and Continuous Refining

You can run the code through main.py

Folder Cache save the calling results of LLMs when parsing 16 benchmark datasets. We can reproduce the parsing results through the data recorded in the Cache folder.

The results are recorded in results folder.

If you want to rerun the code with new large language model, please modify line 86 in the logparser/LogReflex.py file and connect the large model by url. Meanwhile, modify the cache and result saving paths in main.py. If you do not modify the cache path, the code will read the existing records.
