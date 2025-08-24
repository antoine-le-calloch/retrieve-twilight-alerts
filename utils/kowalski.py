from penquins import Kowalski
from utils.config import get_config

cfg = get_config()

DEFAULT_PROCESSES = 4

def get_kowalski_credentials() -> dict:
    protocol = cfg["kowalski.protocol"]
    host = cfg["kowalski.host"]
    port = cfg["kowalski.port"]
    token = cfg["kowalski.token"]

    if protocol not in ("http", "https"):
        raise ValueError("protocol must be either http or https")

    if not host:
        raise ValueError("host must not be empty")

    if not isinstance(port, int) or port <= 0:
        raise ValueError("port must be a positive integer")

    if not isinstance(token, str) or not token:
        raise ValueError("token must not be empty")

    return {
        "protocol": protocol,
        "host": host,
        "port": port,
        "token": token
    }

credentials = get_kowalski_credentials()

def connect_kowalski(credentials: dict, verbose: bool = False, timeout: int = 6000):
    """Connect to Kowalski

    Args:
        credentials (dict): Kowalski credentials
        verbose (bool, optional): verbose. Defaults to False.
        timeout (int, optional): timeout. Default to 6000.

    Returns:
        Kowalski: Kowalski client
    """
    kowalski = Kowalski(
        protocol=credentials["protocol"],
        host=credentials["host"],
        port=credentials["port"],
        token=credentials["token"],
        verbose=verbose,
        timeout=timeout,
    )
    return kowalski


def get_kowalski(verbose: bool = False):
    """Get Kowalski client

    Returns:
        Kowalski: Kowalski client
    """
    return connect_kowalski(get_kowalski_credentials(), verbose=verbose)

def run_queries(
    kowalski: Kowalski,
    queries: list[dict],
):
    """Run queries in Kowalski and return results.

    Args:
        kowalski (Kowalski): Kowalski client
        queries (list[dict]): list of queries

    Returns:
        list: list of results
    """
    query_response = kowalski.query(
        queries=queries,
        use_batch_query=True,
        max_n_threads=cfg.get("kowalski.n_processes") or DEFAULT_PROCESSES,
    )
    responses = query_response.get("default", [])
    data = []
    for response in responses:
        if response.get("status", {}) != "success":
            print(f"Query failed: {response.get('status', '')}\n Message: {response.get('message', '')}")
        else:
            data.extend(response.get("data", []))
    return data
