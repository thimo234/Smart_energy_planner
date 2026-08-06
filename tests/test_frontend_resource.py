import asyncio
import types
import unittest

from test_support import install_package_stub

install_package_stub()

from custom_components.smart_energy_planner.frontend_resource import (
    async_load_and_upsert_card_resource,
    async_upsert_card_resource,
    lovelace_uses_storage_resources,
)


class _Resources:
    loaded = True

    def __init__(self, items=None):
        self.items = list(items or [])
        self.created = []
        self.updated = []

    def async_items(self):
        return self.items

    async def async_create_item(self, data):
        self.created.append(data)

    async def async_update_item(self, item_id, updates):
        self.updated.append((item_id, updates))


class _InitiallyUnloadedResources(_Resources):
    loaded = False

    async def async_get_info(self):
        self.loaded = True


class FrontendResourceTest(unittest.TestCase):
    def test_current_home_assistant_resource_mode_is_detected(self):
        self.assertTrue(
            lovelace_uses_storage_resources(types.SimpleNamespace(resource_mode="storage"))
        )

    def test_empty_resources_create_card(self):
        resources = _Resources()

        result = asyncio.run(
            async_upsert_card_resource(
                resources,
                card_filename="smart-energy-planner-card.js",
                default_base_url="/smart_energy_planner/smart-energy-planner-card.js",
                version="abc123",
            )
        )

        self.assertEqual(result, "created")
        self.assertEqual(
            resources.created,
            [{
                "res_type": "module",
                "url": "/smart_energy_planner/smart-energy-planner-card.js?v=abc123",
            }],
        )

    def test_existing_hacs_resource_is_updated_in_place(self):
        resources = _Resources([{
            "id": "resource-1",
            "url": "/hacsfiles/ha_energy_planner/smart-energy-planner-card.js?v=old",
            "type": "module",
        }])

        result = asyncio.run(
            async_upsert_card_resource(
                resources,
                card_filename="smart-energy-planner-card.js",
                default_base_url="/smart_energy_planner/smart-energy-planner-card.js",
                version="new456",
            )
        )

        self.assertEqual(result, "updated")
        self.assertEqual(resources.created, [])
        self.assertEqual(
            resources.updated,
            [("resource-1", {
                "res_type": "module",
                "url": "/hacsfiles/ha_energy_planner/smart-energy-planner-card.js?v=new456",
            })],
        )

    def test_existing_internal_resource_is_updated_after_storage_load(self):
        resources = _InitiallyUnloadedResources([{
            "id": "resource-2",
            "url": "/smart_energy_planner/smart-energy-planner-card.js?v=old",
            "type": "module",
        }])

        result = asyncio.run(
            async_load_and_upsert_card_resource(
                resources,
                card_filename="smart-energy-planner-card.js",
                default_base_url="/smart_energy_planner/smart-energy-planner-card.js",
                version="latest789",
            )
        )

        self.assertEqual(result, "updated")
        self.assertTrue(resources.loaded)
        self.assertEqual(resources.created, [])
        self.assertEqual(
            resources.updated,
            [("resource-2", {
                "res_type": "module",
                "url": "/smart_energy_planner/smart-energy-planner-card.js?v=latest789",
            })],
        )


if __name__ == "__main__":
    unittest.main()
