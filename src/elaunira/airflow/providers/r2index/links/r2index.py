"""R2Index operator extra links."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperatorLink

if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey


class R2IndexFileLink(BaseOperatorLink):
    """
    Link to the R2Index file details.

    This link extracts the file ID from the operator's XCom return value
    and constructs a URL to view the file in the R2Index UI.
    """

    name = "R2Index File"

    def get_link(
        self,
        operator: Any,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        """Get the link to the R2Index file."""
        from airflow.models import XCom

        result = XCom.get_value(ti_key=ti_key, key="return_value")
        if result and isinstance(result, dict):
            file_id = result.get("id") or result.get("file_record", {}).get("id")
            if file_id:
                return f"https://r2index.elaunira.com/files/{file_id}"
        return ""
