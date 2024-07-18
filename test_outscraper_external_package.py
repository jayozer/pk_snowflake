from outscraper import ApiClient

def test_outscraper_setup():
    try:
        # Attempt to create an ApiClient instance
        client = ApiClient(api_key='<outscraper_api_key>')
        
        # Check if we can access a method of the ApiClient
        if hasattr(client, 'google_maps_reviews'):
            return "Outscraper package successfully imported and ApiClient instantiated."
        else:
            return "Outscraper package imported, but ApiClient seems to be missing expected methods."
    except Exception as e:
        return f"Error testing outscraper package: {str(e)}"

""" 
# This function will be the handler
The main function doesn't require a session parameter, which aligns with Snowflake's expectations for a Python UDF (User-Defined Function).
It calls test_outscraper_setup, prints the result, and returns it, which should display in Snowflake's output.
"""
def main():
    result = test_outscraper_setup()
    print(result)
    return result