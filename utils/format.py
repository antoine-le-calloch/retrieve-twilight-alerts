from astropy.time import Time

def format_time(time: Time) -> str:
    """Format an astropy Time object to a string suitable for filenames.
    Args:
        time (astropy.time.Time): Time object to format.
    Returns:
        str: Formatted time string.
    """
    return time.to_datetime().strftime("%Y-%m-%d_%H-%M-%S")

def parse_time(time_str: str) -> Time:
    """Parse a time string to an astropy Time object.
    Args:
        time_str (str): Time string to parse.
    Returns:
        astropy.time.Time: Parsed Time object.
    """
    return Time.strptime(time_str, "%Y-%m-%d_%H-%M-%S", format='iso', scale="utc")