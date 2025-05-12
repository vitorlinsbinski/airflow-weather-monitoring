import pytz

def format_date_to_utc(timestamp, format):
    format_tz = pytz.timezone(format)
    date_local = timestamp.astimezone(format_tz)
    
    timestamp_formated = date_local.strftime('%Y%m%d_%H%M%S')

    return timestamp_formated