import os

import dotenv
import dune_client.client
import pandas as pd


dotenv.load_dotenv(".env")

dune_api_key = os.getenv("DUNE_API_KEY")
dune_query_id = os.getenv("DUNE_QUERY_ID")

dune = dune_client.client.DuneClient(
    api_key=dune_api_key,
    request_timeout=300,
)

query_result_df = dune.get_latest_result_dataframe(4322613)

query_result_df.to_csv("pumpfun_creations.csv")

