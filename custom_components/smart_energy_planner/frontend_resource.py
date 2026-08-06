"""Helpers for keeping the bundled Lovelace card resource current."""

from __future__ import annotations

from typing import Any


def lovelace_uses_storage_resources(lovelace: Any) -> bool:
    """Return whether Lovelace resources are managed through storage."""

    resource_mode = getattr(lovelace, "resource_mode", None)
    if resource_mode is None:
        # Compatibility with Home Assistant versions before resource_mode was
        # split from the dashboard mode.
        resource_mode = getattr(lovelace, "mode", None)
    return resource_mode == "storage"


async def async_upsert_card_resource(
    resources: Any,
    *,
    card_filename: str,
    default_base_url: str,
    version: str,
) -> str:
    """Create or update every matching Lovelace card resource."""

    existing = [
        item
        for item in (resources.async_items() or [])
        if str(item.get("url", "")).split("?", maxsplit=1)[0].rstrip("/").endswith(
            f"/{card_filename}"
        )
    ]

    if existing:
        for item in existing:
            existing_base_url = str(item.get("url", "")).split("?", maxsplit=1)[0]
            resource_url = f"{existing_base_url}?v={version}"
            if item.get("url") != resource_url and hasattr(resources, "async_update_item"):
                await resources.async_update_item(
                    item["id"],
                    {
                        "res_type": "module",
                        "url": resource_url,
                    },
                )
        return "updated"

    if hasattr(resources, "async_create_item"):
        await resources.async_create_item(
            {
                "res_type": "module",
                "url": f"{default_base_url}?v={version}",
            }
        )
        return "created"

    return "unsupported"


async def async_load_and_upsert_card_resource(
    resources: Any,
    *,
    card_filename: str,
    default_base_url: str,
    version: str,
) -> str:
    """Load the resource collection and then create or update the card."""

    if not getattr(resources, "loaded", True):
        if hasattr(resources, "async_get_info"):
            await resources.async_get_info()
        if not getattr(resources, "loaded", True):
            return "not_loaded"

    return await async_upsert_card_resource(
        resources,
        card_filename=card_filename,
        default_base_url=default_base_url,
        version=version,
    )
