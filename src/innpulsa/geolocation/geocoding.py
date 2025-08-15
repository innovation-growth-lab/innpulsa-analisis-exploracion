"""Geocoding functionality using Google's Geocoding API."""

import asyncio
import urllib.parse
from typing import Any
import aiohttp
from tqdm import tqdm
from pathlib import Path
import json

from innpulsa.logging import configure_logger
from innpulsa.rate_limiter import RateLimiter  # shared implementation


logger = configure_logger("innpulsa.geolocation.geocoding")


class GoogleGeocoder:
    """handles geocoding requests to Google's API with rate limiting and retries."""

    def __init__(self, api_key: str, calls_per_second: float = 0.25):
        self.api_key = api_key
        self._rate_limiter = RateLimiter(calls_per_second)
        self._session = None

    async def __aenter__(self):
        """Set up async context.

        Returns:
            self

        """
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Clean up async context.

        Args:
            exc_type: exception type
            exc_val: exception value
            exc_tb: exception traceback

        """
        if self._session:
            await self._session.close()

    async def _wait_for_rate_limit(self):
        """Enforce rate limiting using the shared RateLimiter."""
        await self._rate_limiter.acquire()

    async def geocode(
        self, address: str, country: str, area: str, city: str
    ) -> tuple[str | None, tuple[float, float] | None]:
        """
        Geocode a single address with components.

        Args:
            address: street address
            country: country name (will be converted to CO)
            area: administrative area (department)
            city: city name

        Returns:
            tuple of (formatted_address, (latitude, longitude)) or None if geocoding failed

        Raises:
            RuntimeError: if the HTTP session is not initialised

        """
        if not all([address, country, area, city]):
            return None, None

        # build components string (use | without encoding)
        components = [
            "country:CO",  # always use CO for Colombia
            f"administrative_area:{area}",
            f"locality:{city}",
        ]
        components_str = "|".join(components)

        # build URL (encode address separately, using %20 for spaces)
        base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "address": address,
            "key": self.api_key,
        }
        encoded_params = urllib.parse.urlencode(params).replace("+", "%20")
        url = f"{base_url}?{encoded_params}&components={components_str}"

        await self._wait_for_rate_limit()

        if self._session is None:
            raise RuntimeError

        try:
            async with self._session.get(url) as response:
                data = await response.json()

                if data["status"] != "OK":
                    logger.warning("geocoding failed: %s", data["status"])
                    return None, None

                gmaps_address = data["results"][0]["formatted_address"]
                location = data["results"][0]["geometry"]["location"]
                return gmaps_address, (location["lat"], location["lng"])

        except (aiohttp.ClientError, KeyError):
            logger.exception("geocoding request failed.")
            return None, None
        finally:
            # Release the rate limiter regardless of success/failure
            await self._rate_limiter.release()

    async def geocode_batch(
        self,
        addresses: dict[str, dict[str, str]],
        max_retries: int = 3,
        *,
        coordinates_json_path: Path | None = None,
        save_every: int = 50,
        max_concurrent: int = 10,  # You can tune this value
    ) -> dict[str, dict[str, Any]]:
        """
        Geocode a batch of addresses with retries.

        Args:
            addresses: dictionary mapping IDs to address components
            max_retries: maximum number of retry attempts
            coordinates_json_path: path to save checkpoint
            save_every: number of addresses between checkpoints
            max_concurrent: maximum number of concurrent requests

        Returns:
            dictionary mapping IDs to (lat, lng) tuples

        """
        results: dict[str, dict[str, Any]] = {}
        retry_delay = 1.0

        semaphore = asyncio.Semaphore(max_concurrent)
        total = len(addresses)

        async def sem_task(id_, addr):
            async with semaphore:
                return await self._geocode_with_retry(id_, addr, max_retries, retry_delay)

        # Prepare all tasks, but only max_concurrent will run at once
        filtered_items = [(id_, addr) for id_, addr in addresses.items() if all(v is not None for v in addr.values())]
        tasks = [asyncio.create_task(sem_task(id_, addr)) for id_, addr in filtered_items]

        with tqdm(total=total, desc="Geocoding addresses") as pbar:
            for processed, task in enumerate(asyncio.as_completed(tasks), start=1):
                id_, res = await task
                results[id_] = res
                pbar.n = processed
                pbar.refresh()
                if coordinates_json_path and processed % save_every == 0:
                    try:
                        coordinates_json_path.write_text(json.dumps(results, indent=2, ensure_ascii=False))
                        logger.debug(
                            "checkpoint saved after %d addresses -> %s",
                            processed,
                            coordinates_json_path,
                        )
                    except OSError as save_exc:
                        logger.warning("failed to write checkpoint: %s", save_exc)

        # Final checkpoint
        if coordinates_json_path:
            try:
                coordinates_json_path.write_text(json.dumps(results, indent=2, ensure_ascii=False))
                logger.info("final checkpoint saved to %s", coordinates_json_path)
            except OSError as save_exc:
                logger.warning("failed to write final checkpoint: %s", save_exc)

        return results

    async def _geocode_with_retry(
        self, id_: str, addr: dict[str, str], max_retries: int, retry_delay: float
    ) -> tuple[str, dict[str, Any]]:
        """
        Geocode an address with retries.

        Args:
            id_: identifier for the address
            addr: dictionary containing address components
            max_retries: maximum number of retry attempts
            retry_delay: delay between retries in seconds

        Returns:
            tuple of (identifier, dictionary containing geocoding result)

        """
        retries = 0
        while retries <= max_retries:
            try:
                gmaps_address, coords = await self.geocode(
                    addr["formatted_address"],
                    addr["country"],
                    addr["area"],
                    addr["city"],
                )
            except (aiohttp.ClientError, KeyError, TimeoutError):
                retries += 1
                if retries <= max_retries:
                    delay = retry_delay * (2 ** (retries - 1))
                    logger.warning("attempt %d failed for %s, retrying in %.1fs", retries, id_, delay)
                    await asyncio.sleep(delay)
                else:
                    logger.exception("all retries failed for %s: ", id_)
                    return id_, {"gmaps_address": None, "coords": None}
            else:
                return id_, {"gmaps_address": gmaps_address, "coords": coords}
        return id_, {"gmaps_address": None, "coords": None}
