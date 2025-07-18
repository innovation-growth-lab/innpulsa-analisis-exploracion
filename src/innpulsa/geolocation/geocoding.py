"""Geocoding functionality using Google's Geocoding API."""

import asyncio
import urllib.parse
from typing import Dict, Optional, Tuple
import aiohttp
from tqdm.asyncio import tqdm
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
        """set up async context."""
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """clean up async context."""
        if self._session:
            await self._session.close()

    async def _wait_for_rate_limit(self):
        """Enforce rate limiting using the shared RateLimiter."""
        await self._rate_limiter.acquire()

    async def geocode(
        self, address: str, country: str, area: str, city: str
    ) -> Tuple[Optional[str], Optional[Tuple[float, float]]]:
        """geocode a single address with components.

        Args:
            address: street address
            country: country name (will be converted to CO)
            area: administrative area (department)
            city: city name

        Returns:
            tuple of (formatted_address, (latitude, longitude)) or None if geocoding failed
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

        try:
            async with self._session.get(url) as response:
                data = await response.json()

                if data["status"] != "OK":
                    logger.warning("geocoding failed: %s", data["status"])
                    return None, None

                gmaps_address = data["results"][0]["formatted_address"]
                location = data["results"][0]["geometry"]["location"]
                return gmaps_address, (location["lat"], location["lng"])

        except Exception as e:  # pylint: disable=W0718
            logger.error("geocoding request failed: %s", str(e))
            return None, None
        finally:
            # Release the rate limiter regardless of success/failure
            await self._rate_limiter.release()

    async def geocode_batch(
        self,
        addresses: Dict[str, Dict[str, str]],
        max_retries: int = 3,
        *,
        checkpoint_path: Optional[Path] = None,
        save_every: int = 500,
    ) -> Dict[str, Tuple[Optional[float], Optional[float]]]:
        """geocode a batch of addresses with retries.

        Args:
            addresses: dictionary mapping IDs to address components
            max_retries: maximum number of retry attempts
            checkpoint_path: path to save checkpoint
            save_every: number of addresses between checkpoints

        Returns:
            dictionary mapping IDs to (lat, lng) tuples
        """
        results: Dict[str, Dict[str, Optional[Tuple[float, float]]]] = {}
        processed = 0  # counter for periodic saves
        retry_delay = 1.0  # initial retry delay in seconds

        async def _geocode_with_retry(id_: str, addr: Dict[str, str]):
            """Worker coroutine that geocodes one address and returns its result."""
            retries = 0
            while retries <= max_retries:
                try:
                    gmaps_address, coords = await self.geocode(
                        addr["formatted_address"],
                        addr["country"],
                        addr["area"],
                        addr["city"],
                    )
                    return id_, {
                        "gmaps_address": gmaps_address,
                        "coords": coords,
                    }
                except Exception as e:  # pylint: disable=W0718
                    retries += 1
                    if retries <= max_retries:
                        delay = retry_delay * (
                            2 ** (retries - 1)
                        )  # exponential backoff
                        logger.warning(
                            "attempt %d failed for %s: %s, retrying in %.1fs",
                            retries,
                            id_,
                            str(e),
                            delay,
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error("all retries failed for %s: %s", id_, str(e))
                        return id_, {
                            "gmaps_address": None,
                            "coords": None,
                        }

        # process all addresses with progress bar
        tasks = [
            _geocode_with_retry(id_, addr)
            for id_, addr in addresses.items()
            if all(v is not None for v in addr.values())
        ]

        for coro in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Geocoding addresses",
        ):
            id_, res = await coro
            results[id_] = res
            processed += 1

            if checkpoint_path and processed % save_every == 0:
                try:
                    checkpoint_path.write_text(
                        json.dumps(results, indent=2, ensure_ascii=False)
                    )
                    logger.debug(
                        "checkpoint saved after %d addresses -> %s",
                        processed,
                        checkpoint_path,
                    )
                except Exception as save_exc:  # pylint: disable=broad-except
                    logger.warning("failed to write checkpoint: %s", save_exc)

        # final checkpoint
        if checkpoint_path:
            try:
                checkpoint_path.write_text(
                    json.dumps(results, indent=2, ensure_ascii=False)
                )
                logger.info("final checkpoint saved to %s", checkpoint_path)
            except Exception as save_exc:  # pylint: disable=broad-except
                logger.warning("failed to write final checkpoint: %s", save_exc)

        return results
