import pytest


@pytest.fixture
def sample_business_data():
    return [
        {
            "Business Name": "Test Restaurant",
            "Phone Number": "(555) 123-4567",
            "Website": "https://testrestaurant.com",
            "Address": "123 Main St, New York, NY 10001",
        },
        {
            "Business Name": "Another Business",
            "Phone Number": "555-987-6543",
            "Website": "https://anotherbusiness.com",
            "Address": "456 Oak Ave, Brooklyn, NY 11201",
        },
    ]


@pytest.fixture
def sample_email_data():
    return [
        {
            "Business Name": "Test Company",
            "Phone Number": "",
            "Website": "https://testcompany.com",
            "Email": "contact@testcompany.com",
            "Source": "Yahoo Search",
        },
        {
            "Business Name": "",
            "Phone Number": "555-111-2222",
            "Website": "",
            "Email": "info@example.com",
            "Source": "Yahoo Search",
        },
    ]
