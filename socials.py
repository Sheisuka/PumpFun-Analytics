import aiohttp
import asyncio
import dotenv
import os
import pandas as pd
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("error.log"),
        logging.StreamHandler()
    ]
)

def get_proxy():
    proxy = os.getenv("PROXY")
    proxy_user = os.getenv("PROXY_USER")
    proxy_pass = os.getenv("PROXY_PASS")
    proxy_auth = aiohttp.BasicAuth(proxy_user, proxy_pass)
    
    assert proxy and proxy_user and proxy_pass, "Proxy is not provided"

    return proxy, proxy_auth


async def fetch_uri(session, uri):
    async with semaphore:
        try:
            async with session.get(uri.strip()) as response:
                if response.status != 200:
                    logging.warning(f"Non-200 response for URL: {uri} (status: {response.status})")
                    return None
                
                try:
                    data = await response.json()
                except Exception as json_error:
                    logging.error(f"JSON decoding error for URL: {uri} - {json_error}")
                    return None

                return data
        except Exception as e:
            logging.error(f"Error fetching URL: {uri} - {e}")
            return None


async def fetch_all(session, uris):
    tasks = []

    for uri in uris:
        task = asyncio.create_task(fetch_uri(session, uri))
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    return results


async def parse_uris(uris):
    data = {
        "telegram": [],
        "website": [],
        "twitter": [],
        "image_uri": [],
        "name": [],
        "symbol": [],
        "description": [],
    }
    
    proxy, proxy_auth = get_proxy()

    async with aiohttp.ClientSession(proxy=proxy, proxy_auth=proxy_auth) as session:
        results = await fetch_all(session, uris)
        for result in results:
            for key in data.keys():
                if result is None:
                    data[key].append(None)
                else:
                    data[key].append(result.get(key))
    
    return data


async def main():
    logging.info("Started.")
    try:
        df = pd.read_csv("pumpfun_creations.csv").head(1000)
        uris = df["uri"]

        offchain_data = await parse_uris(uris)

        offchain_df = pd.DataFrame(offchain_data)
        extended_df = pd.concat([df, offchain_df], axis=1)
        extended_df.to_csv("extended_data.csv", index=False)

        logging.info("Parsing completed successfully. Data saved to 'extended_data.csv'.")
    except FileNotFoundError as fnf_error:
        logging.error(f"CSV file not found: {fnf_error}")
    except Exception as e:
        logging.error(f"Unexpected error in main: {e}")

    logging.info("Finished.")


dotenv.load_dotenv(".env")

semaphore = asyncio.Semaphore(5)
asyncio.run(main())
