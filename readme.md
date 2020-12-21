# Instructions

1. The script runs on PySpark and its dependencies. To set up the environment, run following command with the attached requirements.yml.
```
conda env create -f requirements.yaml
```

2. If you run the code on Windows environment, you may experience following or similar errors (https://stackoverflow.com/questions/40764807/null-entry-in-command-string-exception-in-saveastextfile-on-pyspark). It is because you are missing winutils.exe a hadoop binary. To resolve the issue, you need to download the winutils.exe file & set your hadoop home pointing to it. Here are instructions (based on answers to similar questions from stackoverflow):
   - Download the bin folder from https://github.com/cdarlint/winutils. Note there are bin folders for various hadoop versions. I used the folder 'hadoop 3.2.0'.
   - Create hadoop folder in Your System, ex C:
   - Paste the bin folder in hadoop directory, ex : C:\hadoop\bin
   - In User Variables in System Properties -> Advance System Settings, Create New Variable Name: HADOOP_HOME Path: C:\hadoop\
   - Restart the python environment and the issue should be resolved.

3. Run the following command to execute the script. Make sure the data files "transactions_full.csv" are placed under the same folder of the script. You may tune the input parameters (including data file path, data ranges and fee ranges) as defined under the main code. The code outputs transactions list and entities list into two folders "suspicious transactions" and "suspicious entities", respectively.
```
python main.py
```
4. Any further questions, reach me at taocao1@gmail.com
