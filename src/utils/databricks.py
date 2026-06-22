"""Databricks workspace helpers."""


def get_repo_root(dbutils) -> str:
    """Derive Repos root from the running notebook path (no widget / no hardcoded email)."""
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    notebook_path = ctx.notebookPath().get()
    if not notebook_path:
        raise ValueError("Could not detect notebook path. Open this notebook from Databricks Repos.")

    if "/notebooks/" not in notebook_path:
        raise ValueError(
            f"Notebook not under notebooks/: {notebook_path}. "
            "Run from the cloned repo in Databricks Repos."
        )

    return notebook_path.split("/notebooks/")[0]
