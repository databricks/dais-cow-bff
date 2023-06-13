# Cow BBF Project

## Setup Virtual Environment

Configure Python virtual environment

```sh
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```


## Setup VS Code

Run command "Prefernced: Open User Settings (JSON)" and add the following block:

```
  "jupyter.runStartupCommands": [ 
    "%load_ext autoreload\n%autoreload 2",
    "global spark\ntry:\n  spark\nexcept NameError:\n  from databricks.connect import DatabricksSession\n  spark = DatabricksSession.builder.getOrCreate()",
    "from pyspark.sql.connect.dataframe import DataFrame\ndef df_html(df):\n  return df.toPandas().to_html()\nhtml_formatter = get_ipython().display_formatter.formatters['text/html']\nhtml_formatter.for_type(DataFrame, df_html)",
    "from databricks.sdk.runtime import *",
    "import sys; sys.path.append('${workspaceFolder}')"
  ],
```
