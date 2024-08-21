# Databricks notebook source
# MAGIC %pip install --force-reinstall typing-extensions==4.5
# MAGIC %pip install --force-reinstall openai==1.8
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

USR_ADRESS=dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
TGT_DIRS = [
  f"/Workspace/Users/{USR_ADRESS}/llm-codereview/src1",
  f"/Workspace/Users/{USR_ADRESS}/llm-codereview/src2",
]
USE_CATALOG = "main"
USE_SCHEMA = "default"
ENDPOINT_NAME="databricks-meta-llama-3-1-70b-instruct"

# COMMAND ----------

RULES =[
      {
        "title": "変数名、関数名、クラス名の命名",
        "is_llm":True,
        "summary": "読みやすく、意味のある名前を使用する。",
        "detail": "変数名はその役割が一目でわかるようにする（例: xやyではなく、ageやprice）。関数名はその機能がわかるようにする（例: func1ではなく、calculate_average）。クラス名はキャメルケースで意味のあるものにする（例: myclassではなく、DataProcessor）。",
        "criteria": "名前の意味がわかりやすいかどうかを自然言語処理を用いて自動チェックします。"
      },
      {
        "title": "コメントの使用",
        "is_llm":True,
        "summary": "コードの理解を助けるためにコメントを適切に使用する。",
        "detail": "重要なロジックやアルゴリズムにはコメントを追加。関数やクラスの定義にはドキュメンテーションコメント（docstring）を追加。",
        "criteria": "主要な関数や複雑なロジックに対してコメントがあるかを自動チェックします。"
      },
      {
        "title": "不要なコードの削除",
        "is_llm":True,
        "summary": "不要なコードやコメントアウトされたコードは削除する。",
        "detail": "使用されていない変数や関数は削除。デバッグ用の一時的なコードやコメントは残さない。",
        "criteria": "使用されていないコードやコメントを静的解析ツールで自動チェックします。"
      }
    ]

# COMMAND ----------

from databricks.sdk import WorkspaceClient
spark.sql(f"USE CATALOG {USE_CATALOG};")
spark.sql(f"USE SCHEMA {USE_SCHEMA};")

# Initialize the WorkspaceClient
w = WorkspaceClient()
# List to store the paths of the Python files
path_list = []
for tgt_dir in TGT_DIRS:
  path_list += [f.path for f in w.workspace.list(tgt_dir, recursive=True)]

# COMMAND ----------

from databricks.sdk.service.workspace import Language, ObjectType, ObjectInfo
from pathlib import Path

# Pythonノートブックが有効かどうかを確認する関数
def is_valid_python_notebook(obj: ObjectInfo):
    return obj.language is Language.PYTHON and obj.object_type is ObjectType.NOTEBOOK

class PythonNotebook:
    def __init__(self, py_file_path: str):
        # ノートブックの内容をダウンロードして読み込む
        with w.workspace.download(py_file_path) as f:
            content = f.read().decode("utf-8")
        self.py_file_path = py_file_path
        # セルを分割してリストに格納
        self.cells = content.replace("# Databricks notebook source\n", "").split(
            "# COMMAND ----------\n\n"
        )

    # 有効なPythonセルを抽出するメソッド
    def extract_python_cells(self):
        cell_data = []
        fname = Path(self.py_file_path).name
        for i, cell in enumerate(self.cells):
            if self.is_valid_python_cell(cell):
                cell_data.append([fname, self.py_file_path, i + 1, cell])
        return cell_data

    # 有効なPythonセルかどうかを確認する静的メソッド
    @staticmethod
    def is_valid_python_cell(cell_str: str):
        python_tgt = ["# MAGIC %md", "# MAGIC %sql"]
        valid = bool(cell_str.strip())
        valid = valid and not any([tgt in cell_str for tgt in python_tgt])
        return valid

# 全てのPythonファイルからセルデータを抽出
cell_data = []
for py_file in path_list:
    obj = w.workspace.get_status(py_file)
    if is_valid_python_notebook(obj):
        cell_data += PythonNotebook(py_file).extract_python_cells()

# Create a DataFrame from the cell data
df = spark.createDataFrame(cell_data, schema=["file_name", "file_path", "cell_number", "cell_content"])

# COMMAND ----------

from pyspark.sql.functions import regexp_replace


# Convert newline characters to <br> and spaces to &nbsp;
df_html = df.withColumn("cell_content_html", regexp_replace("cell_content", "\n", "<br>"))
df_html = df_html.withColumn("cell_content_html", regexp_replace("cell_content_html", " ", "&nbsp;"))

# Save the DataFrame as a table named "python_cells"
df_html.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("python_cells")

# COMMAND ----------

rule_df = spark.createDataFrame(RULES).write.mode("overwrite").option("mergeSchema", "true").saveAsTable("rule_df")

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def get_prompt(rule, code):
    prompt = f"""
    あなたはシニアなPythonエンジニアです。チームの作成した<コード>を<観点>のポイントについてレビューして、ルールを守れているかのレビュー結果を返してください。
    他の観点でのレビューは不要です。
    出力結果は自動的に処理されるため、必ず<応答形式>を遵守してください
    <観点>
    {rule}
    <コード>
    {code}
    <応答形式>
    以下の要素を持つJSONフォーマットのStructを一つだけ（リストにせず）文字列で応答してください
    "is_violate":コーディングルール違反に該当するかどうか true or false
    "line_number_range":何行目が該当するのか、[start end]のリスト形式で行数範囲を表す
    "detail":どのようにコーディングルールに該当するか・どのように改善するべきか 
    """
    return prompt

get_prompt_udf = udf(get_prompt, StringType())

# COMMAND ----------

from pyspark.sql.functions import from_json, col,expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    ArrayType,
)

# Define the schema for the JSON structure
schema = StructType(
    [
        StructField("is_violate", BooleanType(), True),
        StructField("line_number_range", ArrayType(IntegerType()), True),
        StructField("detail", StringType(), True),
    ]
)

reveiew_df =(
    spark.table("python_cells")
    .crossJoin(spark.table("rule_df"))
    .withColumn("prompt", get_prompt_udf(col("rule_df.detail"), col("python_cells.cell_content")))
    .withColumn("review_result", expr(f'ai_query("{ENDPOINT_NAME}", prompt)'))
    .withColumn("review_result_struct", from_json(col("review_result"), schema)
) 
 ) # Convert the JSON column to a struct
reveiew_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("review_result")
