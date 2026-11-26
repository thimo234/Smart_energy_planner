"""Manual feedback on an accuplanner's published plan."""

from __future__ import annotations

from asyncio import Lock
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from homeassistant.components.button import ButtonEntity
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import CONF_PLANNER_KIND, DOMAIN, PLANNER_KIND_BATTERY
from .household import async_setup_platform

FEEDBACK_LIMIT = 50
_FEEDBACK_FIELDS = (
    "forecast_generated_at", "status", "rationale", "price_resolution",
    "battery_planner_engine", "battery_planner_engine_reason",
    "battery_soc_percent", "battery_min_soc_percent", "battery_total_energy_kwh",
    "battery_reserved_energy_kwh", "battery_min_profit_per_kwh",
    "battery_charge_cycle_active", "battery_discharge_cycle_active", "source_errors",
    "upcoming_energy_price_windows", "upcoming_export_price_windows",
    "estimated_hourly_solar_forecast", "estimated_hourly_home_demand",
    "planned_grid_charge_windows", "planned_solar_charge_windows",
    "planned_battery_mode_windows", "estimated_battery_mode_windows",
)


async def async_setup_entry(hass, entry, async_add_entities) -> None:
    """Set up feedback for standalone and household battery planners."""
    await async_setup_platform(hass, entry, async_add_entities, _async_setup_buttons)


async def _async_setup_buttons(hass, entry, async_add_entities) -> None:
    coordinator = hass.data[DOMAIN][entry.entry_id]
    if coordinator._config.get(CONF_PLANNER_KIND, PLANNER_KIND_BATTERY) != PLANNER_KIND_BATTERY:
        return
    feedback = PlanFeedback(hass, entry.entry_id, coordinator)
    await feedback.async_load()
    async_add_entities([PlanFeedbackButton(entry, feedback, reject=False),
                        PlanFeedbackButton(entry, feedback, reject=True)])


class PlanFeedback:
    """Serialize votes for one planner and keep them separate from runtime state."""

    def __init__(self, hass, identity: str, coordinator) -> None:
        self.coordinator = coordinator
        self._store: Store[dict[str, Any]] = Store(hass, 1, f"{DOMAIN}.plan_feedback.{identity}")
        self._lock = Lock()

    async def async_load(self) -> None:
        data = await self._store.async_load() or {}
        self.coordinator._battery_plan_feedback = tuple(data.get("records", []))

    async def async_vote(self, *, reject: bool) -> None:
        plan = self.coordinator.data
        if plan is None or plan.battery_planner_engine != "v2":
            raise HomeAssistantError("No battery plan is available to rate")
        # Take the snapshot before the first await: an automatic refresh may
        # replace the displayed plan while this vote is being saved.
        record = {
            "timestamp": datetime.now(UTC).isoformat(),
            "vote": "rejected" if reject else "approved",
            "plan": deepcopy({field: getattr(plan, field, None) for field in _FEEDBACK_FIELDS}),
        }
        async with self._lock:
            data = await self._store.async_load() or {}
            records = [*data.get("records", []), record][-FEEDBACK_LIMIT:]
            await self._store.async_save({"records": records})
            self.coordinator._battery_plan_feedback = tuple(records)
        if reject:
            await self.coordinator.async_request_refresh()


class PlanFeedbackButton(CoordinatorEntity, ButtonEntity):
    """A vote on the currently published plan for this planner only."""

    _attr_has_entity_name = True

    def __init__(self, entry, feedback: PlanFeedback, *, reject: bool) -> None:
        super().__init__(feedback.coordinator)
        self._feedback = feedback
        self._reject = reject
        key = "reject_plan" if reject else "approve_plan"
        self._attr_unique_id = f"{entry.entry_id}_{key}"
        self._attr_translation_key = key
        self._attr_icon = "mdi:refresh" if reject else "mdi:thumb-up-outline"
        if hasattr(entry, "subentry_id"):
            self._attr_device_info = {
                "identifiers": {(DOMAIN, entry.entry_id)}, "name": entry.title,
                "manufacturer": "Smart Energy Planner", "model": PLANNER_KIND_BATTERY,
                "via_device": (DOMAIN, entry.parent.entry_id),
            }
        else:
            self._attr_translation_key = f"{key}_standalone"
            self._attr_translation_placeholders = {"planner": entry.title}

    async def async_press(self) -> None:
        """Save the rating and request a normal safe refresh for a rejection."""
        await self._feedback.async_vote(reject=self._reject)
