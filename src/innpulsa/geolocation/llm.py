"""this script preprocesses locations using LLMs."""

import asyncio
import json
import os
from pathlib import Path
import random
from functools import wraps
from typing import Dict, List, Any, TypeVar, Callable, Awaitable
import pandas as pd
from google import genai

from innpulsa.logging import configure_logger

logger = configure_logger("innpulsa.geolocation.llm")
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


# Type variable for the retry decorator
T = TypeVar("T")


def with_exponential_backoff(
    max_retries: int = 5,
    initial_delay: float = 1.0,
    exponential_base: float = 2.0,
    jitter: float = 0.1,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorator that implements exponential backoff for async functions.

    Args:
        max_retries: Maximum number of retries before giving up
        initial_delay: Initial delay between retries in seconds
        exponential_base: Base for the exponential backoff
        jitter: Random jitter factor to add to delay

    Returns:
        Decorated function with retry logic
    """

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None
            delay = initial_delay

            for retry in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:  # pylint: disable=W0718
                    last_exception = e
                    if retry < max_retries - 1:  # Don't log on last attempt
                        logger.warning(
                            "attempt %d/%d failed: %s, retrying in %.2f seconds",
                            retry + 1,
                            max_retries,
                            str(e),
                            delay,
                        )
                        # Add jitter to prevent thundering herd
                        jitter_delay = delay * (1 + random.uniform(-jitter, jitter))
                        await asyncio.sleep(jitter_delay)
                        delay *= exponential_base
                    continue

            # If we get here, we've exhausted our retries
            logger.error(
                "all %d retry attempts failed, last error: %s",
                max_retries,
                str(last_exception),
            )
            raise last_exception

        return wrapper

    return decorator


def format_addresses_for_prompt(addresses: Dict[str, str]) -> str:
    """format addresses dictionary into a JSON string for the prompt.

    Args:
        addresses: dictionary mapping address IDs to address strings

    Returns:
        JSON string representation of the addresses dictionary
    """
    # Convert Python dict to JSON string with proper formatting
    return json.dumps(addresses, indent=2, ensure_ascii=False)


class RateLimiter:
    """rate limiter that ensures minimum delay between operations."""

    active_batches = 0  # Track concurrent batches

    def __init__(self, calls_per_second: float = 1.0):
        self.min_interval = 1.0 / calls_per_second
        self.last_call_time = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        """wait until we can make another call."""
        async with self._lock:
            RateLimiter.active_batches += 1
            logger.info("active batches: %d", RateLimiter.active_batches)
            now = asyncio.get_event_loop().time()
            time_since_last_call = now - self.last_call_time
            if time_since_last_call < self.min_interval:
                wait_time = self.min_interval - time_since_last_call
                await asyncio.sleep(wait_time)
            self.last_call_time = asyncio.get_event_loop().time()

    async def release(self):
        """release the rate limiter."""
        async with self._lock:
            RateLimiter.active_batches -= 1
            logger.info("active batches: %d", RateLimiter.active_batches)


def clean_json_response(response_text: str) -> str:
    """Clean and validate JSON response from LLM.

    Args:
        response_text: Raw response text from LLM

    Returns:
        Cleaned JSON string
    """
    # Remove any markdown code block markers
    text = response_text.replace("```json", "").replace("```", "")

    # Remove any leading/trailing whitespace and newlines
    text = text.strip()

    # Validate it's parseable JSON (will raise JSONDecodeError if not)
    json.loads(text)  # validation only

    return text


@with_exponential_backoff(max_retries=5, initial_delay=1.0)
async def make_llm_request(formatted_addresses: str, prompt: str) -> str:
    """Make a request to the LLM with retry logic.

    Args:
        formatted_addresses: JSON string of addresses to process
        prompt: The prompt to use for the LLM request

    Returns:
        Response text from LLM
    """
    response = await asyncio.to_thread(
        client.models.generate_content,
        model="gemini-2.0-flash",
        contents=prompt.format(batch_addresses=formatted_addresses),
    )
    return response.text


async def process_address_batch(
    addresses: Dict[str, str],
    rate_limiter: RateLimiter,
    batch_id: int,
    prompt: str,
) -> Dict[str, Any]:
    """process a batch of addresses using the LLM with rate limiting.

    Args:
        addresses: dictionary mapping address IDs to address strings
        rate_limiter: rate limiter instance
        batch_id: unique identifier for this batch
        prompt: The prompt to use for the LLM request

    Returns:
        dictionary containing batch results
    """
    try:
        logger.debug("processing batch %d with %d addresses", batch_id, len(addresses))

        await rate_limiter.acquire()
        formatted_addresses = format_addresses_for_prompt(addresses)

        # Use the retrying request function
        response_text = await make_llm_request(formatted_addresses, prompt)

        # Clean and validate the response
        cleaned_response = clean_json_response(response_text)

        result = {
            "batch_id": batch_id,
            "status": "success",
            "input_addresses": addresses,
            "response": cleaned_response,  # Store cleaned response
            "processed_count": len(addresses),
        }

        logger.debug("successfully processed batch %d", batch_id)
        return result

    except Exception as e:  # pylint: disable=W0718
        logger.error("failed to process batch %d: %s", batch_id, str(e))
        return {
            "batch_id": batch_id,
            "status": "error",
            "input_addresses": addresses,
            "error": str(e),
            "processed_count": 0,
        }
    finally:
        await rate_limiter.release()


def create_address_batches(
    df: pd.DataFrame, batch_size: int = 25
) -> List[Dict[str, str]]:
    """create batches of addresses from DataFrame.

    Args:
        df: DataFrame containing address data with UniqueID and full_address columns
        batch_size: size of each batch

    Returns:
        list of address batches
    """
    batches = []

    # ensure we have both required columns
    if "numberid_emp1" not in df.columns:
        logger.error("missing numberid_emp1 column in input data")
        return []

    # create dictionary mapping UniqueID to address
    addresses = dict(zip(df["numberid_emp1"], df["full_address"].fillna("")))

    # filter out empty addresses
    addresses = {id_: addr for id_, addr in addresses.items() if addr.strip()}

    # convert to list of items for batching
    address_items = list(addresses.items())

    for i in range(0, len(address_items), batch_size):
        batch = dict(address_items[i : i + batch_size])
        batches.append(batch)

    logger.info("created %d batches from %d addresses", len(batches), len(addresses))
    return batches


def save_batch_result(result: Dict[str, Any], output_dir: Path) -> None:
    """save batch result to JSON file.

    Args:
        result: batch processing result
        output_dir: directory to save results
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    batch_id = result["batch_id"]
    status = result["status"]
    filename = f"batch_{batch_id:04d}_{status}.json"

    output_path = output_dir / filename

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    logger.debug("saved batch %d result to %s", batch_id, output_path)


async def normalise_addresses_using_llm(
    df: pd.DataFrame,
    output_dir: Path,
    prompt: str,
    batch_size: int = 10,
    calls_per_second: float = 0.25,  # 15 requests per minute
) -> Dict[str, int]:
    """process all ZASCA addresses using LLM with rate limiting.

    Args:
        df: DataFrame containing ZASCA data with full_address column
        output_dir: directory to save batch results
        batch_size: size of each batch
        calls_per_second: number of API calls allowed per second (default: 0.25,
            which is 15 requests/minute)
        prompt: The prompt to use for the LLM request

    Returns:
        summary statistics
    """
    logger.info(
        "starting address processing with batch_size=%d, calls_per_second=%.2f",
        batch_size,
        calls_per_second,
    )

    # create batches
    batches = create_address_batches(df, batch_size)

    if not batches:
        logger.warning("no addresses found to process")
        return {"total_batches": 0, "successful_batches": 0, "failed_batches": 0}

    # initialise rate limiter
    rate_limiter = RateLimiter(calls_per_second)

    # process all batches concurrently
    tasks = [
        process_address_batch(batch, rate_limiter, i, prompt)
        for i, batch in enumerate(batches)
    ]

    logger.info("processing %d batches...", len(tasks))
    results = await asyncio.gather(*tasks)

    # save all results
    successful_batches = 0
    failed_batches = 0

    for result in results:
        save_batch_result(result, output_dir)

        if result["status"] == "success":
            successful_batches += 1
        else:
            failed_batches += 1

    logger.info(
        "completed processing: %d successful, %d failed out of %d total batches",
        successful_batches,
        failed_batches,
        len(batches),
    )

    return {
        "total_batches": len(batches),
        "successful_batches": successful_batches,
        "failed_batches": failed_batches,
    }


# legacy function for backwards compatibility
async def process_addresses(
    addresses: dict[str, str], prompt: str
) -> dict[str, dict[str, str]]:
    """process a batch of addresses using the LLM."""
    formatted_addresses = format_addresses_for_prompt(addresses)
    response = await client.models.generate_content(
        model="gemini-2.0-flash",
        contents=prompt.format(addresses=formatted_addresses),
    )
    return response.text
