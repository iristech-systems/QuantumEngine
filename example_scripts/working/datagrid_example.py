"""
Example: Using SurrealEngine DataGrid helpers to optimize your routes

This shows how to replace your current Python-based filtering with efficient SurrealDB queries.
"""

import asyncio
from quantumengine import (
    get_grid_data, 
    parse_datatables_params, 
    format_datatables_response,
    Document, 
    StringField, 
    DateTimeField, 
    DecimalField, 
    BooleanField,
    create_connection
)


# Example models (matching your use case)
class Listing(Document):
    marketplace = StringField()
    seller_name = StringField()
    product_name = StringField()
    product_sku_model_number = StringField()
    currency_code = StringField()
    offer_price = DecimalField()
    product_umap = DecimalField()
    product_pmap = DecimalField()
    product_msrp = DecimalField()
    percent_difference = DecimalField()
    date_collected = DateTimeField()
    below_map = BooleanField()
    above_map = BooleanField()
    buy_page_url = StringField()
    screenshot_url = StringField()

    class Meta:
        collection = "listing"


class CaseEvidence(Document):
    evidence_type = StringField()
    source_type = StringField()
    timestamp = DateTimeField()
    document_url = StringField()
    source_data = StringField()  # JSON field

    class Meta:
        collection = "case_evidence"


# Example: Your optimized route using SurrealDB queries
async def optimized_case_listings_route(case_id: str, request_args: dict):
    """
    Optimized version of your listings route using SurrealDB queries
    
    Instead of:
    1. Getting ALL listings
    2. Filtering in Python
    3. Paginating in Python
    
    This does:
    1. Apply filters in SurrealDB
    2. Get only the needed page from SurrealDB
    """
    
    # Your case lookup logic stays the same
    # case = await Case.get(case_id)
    # if not case:
    #     return {"error": "Case not found"}
    
    # Define which fields can be searched
    search_fields = [
        'marketplace', 
        'seller_name', 
        'product_sku_model_number', 
        'product_name'
    ]
    
    # Define custom filters (URL param -> database field mapping)
    custom_filters = {
        'marketplace': 'marketplace',
        'seller': 'seller_name'
    }
    
    # Use the helper to get efficiently queried data
    result = await get_grid_data(
        Listing,                    # Your document class
        request_args,               # request.args.to_dict()
        search_fields,              # Fields to search in
        custom_filters,             # Custom filter mappings
        default_sort='date_collected'  # Default sort field
    )
    
    return result  # Already in {"total": total, "rows": rows} format!


async def optimized_case_evidence_route(case_id: str, request_args: dict):
    """
    Optimized version of your evidence route
    """
    
    search_fields = ['source_type']  # Note: source_data.product would need special handling
    
    custom_filters = {
        'evidence_type': 'evidence_type',
        'source_type': 'source_type'
    }
    
    result = await get_grid_data(
        CaseEvidence,
        request_args,
        search_fields,
        custom_filters,
        default_sort='-timestamp'  # DESC sort
    )
    
    return result


# Example: DataTables support
async def datatables_listings_route(case_id: str, request_args: dict):
    """
    Example route for DataTables which uses different parameters
    """
    
    # Convert DataTables params to standard format
    params = parse_datatables_params(request_args)
    
    # Get data using standard helper
    search_fields = ['marketplace', 'seller_name', 'product_name']
    custom_filters = {'marketplace': 'marketplace', 'seller': 'seller_name'}
    
    result = await get_grid_data(
        Listing,
        params,  # Uses converted parameters
        search_fields,
        custom_filters
    )
    
    # Format for DataTables
    return format_datatables_response(
        result['total'], 
        result['rows'], 
        params['draw']
    )


# Example: Usage in your actual route
def example_quart_route():
    """
    Example of how your route would look:
    
    @case_management_blueprint.route('/api/cases/<case_id>/listings/optimized')
    async def api_case_listings_optimized(case_id):
        # Your case validation
        case = await Case.get(case_id)
        if not case:
            return jsonify({"error": "Case not found"}), 404

        # One line replaces all your filtering/pagination logic!
        result = await get_grid_data(
            Listing,
            request.args.to_dict(),
            search_fields=['marketplace', 'seller_name', 'product_name', 'product_sku_model_number'],
            custom_filters={'marketplace': 'marketplace', 'seller': 'seller_name'}
        )
        
        return jsonify(result)
    
    
    @case_management_blueprint.route('/api/cases/<case_id>/listings/datatables')
    async def api_case_listings_datatables(case_id):
        # DataTables support
        case = await Case.get(case_id)
        if not case:
            return jsonify({"error": "Case not found"}), 404

        params = parse_datatables_params(request.args.to_dict())
        
        result = await get_grid_data(
            Listing,
            params,
            search_fields=['marketplace', 'seller_name', 'product_name'],
            custom_filters={'marketplace': 'marketplace', 'seller': 'seller_name'}
        )
        
        datatables_result = format_datatables_response(
            result['total'], 
            result['rows'], 
            params['draw']
        )
        
        return jsonify(datatables_result)
    """
    pass


async def main():
    """Demo the functionality"""
    
    # Create connection (you'd use your existing connection)
    connection = create_connection(
        url="ws://localhost:8000/rpc",
        namespace="test_ns",
        database="test_db",
        username="root",
        password="root",
        make_default=True,
        async_mode=True  # This is the key parameter for using sync API
    )
    await connection.connect()

    # Mock request args (simulating your route parameters)
    mock_request_args = {
        'limit': '25',
        'offset': '0',
        'search': 'amazon',
        'marketplace': 'Amazon',
        'seller': ''
    }
    
    # Demo the optimized query
    try:
        result = await optimized_case_listings_route("test_case", mock_request_args)
        print("Optimized query result:")
        print(f"Total: {result['total']}")
        print(f"Rows returned: {len(result['rows'])}")
        
        # Demo DataTables conversion
        datatables_args = {
            'start': '0',
            'length': '10', 
            'draw': '1',
            'search[value]': 'test'
        }
        
        params = parse_datatables_params(datatables_args)
        print(f"\nDataTables params converted: {params}")
        
    except Exception as e:
        print(f"Demo error (expected if no data): {e}")
    
    await connection.disconnect()


if __name__ == "__main__":
    asyncio.run(main())