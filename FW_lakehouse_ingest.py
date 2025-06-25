# ======================= Spark helper to load Δ-tables and run SQL =======================
from typing import List
from pyspark.sql import DataFrame

def create_spark_tables(
    workspace: str,
    lakehouse_layer: str,
    table_names: List[str],
    sql_script: str,
) -> DataFrame:
    """
    Reads Delta tables from a Microsoft Fabric / OneLake Lakehouse, registers
    each as a temporary Spark SQL view, and executes a SQL statement.

    Parameters
    ----------
    workspace : str
        The Fabric workspace name (e.g. ``"my_fabric_workspace"``).
    lakehouse_layer : str
        Logical layer within the Lakehouse
        (e.g. ``"landing"``, ``"curated"``, ``"analytics"``).
    table_names : list[str]
        List of Delta-table names to register as views.
    sql_script : str
        The Spark SQL query to run once all views are in place.

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame produced by executing *sql_script*.

    Examples
    --------
    >>> sql = \"\"\"
    ... SELECT customer_id, created_at
    ... FROM customer_snapshot
    ... WHERE created_at >= '2025-01-01'
    ... \"\"\"
    >>> df = create_spark_tables(
    ...     workspace="my_fabric_workspace",
    ...     lakehouse_layer="landing",
    ...     table_names=["customer_snapshot"],
    ...     sql_script=sql,
    ... )
    >>> df.show()
    """
    try:
        lakehouse_layer = lakehouse_layer.lower()

        print(f"Workspace         : {workspace}")
        print(f"Lakehouse layer   : {lakehouse_layer}")
        print(f"Tables to register: {table_names}")

        # ------------------------------------------------------------------ #
        # Register each table as a temporary view                            #
        # ------------------------------------------------------------------ #
        for tbl in table_names:
            tbl_lower = tbl.lower()

            try:
                table_path = (
                    f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/"
                    f"{lakehouse_layer}.Lakehouse/Tables/{tbl_lower}"
                )

                df = spark.read.format("delta").load(table_path)
                df.createOrReplaceTempView(tbl_lower)

                print(f"✔️  Registered '{tbl_lower}' ({df.count()} rows)")

            except Exception as table_err:
                # Log the error but continue attempting the remaining tables
                print(f"❌  Could not register '{tbl_lower}': {table_err}")

        # ------------------------------------------------------------------ #
        # Execute the user-supplied SQL against the registered temp views    #
        # ------------------------------------------------------------------ #
        result_df = spark.sql(sql_script)

        print(f"✅  Query complete — {result_df.count()} rows returned")
        result_df.printSchema()

        return result_df

    except Exception as err:
        print(f"create_spark_tables failed: {err}")
        raise
