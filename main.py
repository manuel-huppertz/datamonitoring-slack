import functions_framework

@functions_framework.http
def my_function(request):
    """HTTP Cloud Function that responds with a greeting."""
    request_json = request.get_json(silent=True)
    
    name = request_json.get("name") if request_json else "World"
    
    return f"Hello, {name}!", 200