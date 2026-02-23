import pytest
import pandas as pd

from lead_scraper import parse_search_prompt, process_and_clean_data


class TestParseSearchPrompt:
    def test_parse_maps_with_in_keyword(self):
        result = parse_search_prompt("restaurants in New York")
        assert result["search_type"] == "maps"
        assert result["keywords"] == "restaurants"
        assert result["location"] == "New York"

    def test_parse_maps_with_near_keyword(self):
        result = parse_search_prompt("plumbers near Brooklyn")
        assert result["search_type"] == "maps"
        assert result["keywords"] == "plumbers"
        assert result["location"] == "Brooklyn"

    def test_parse_maps_default_location(self, monkeypatch):
        monkeypatch.setenv("DEFAULT_LOCATION", "Los Angeles")
        result = parse_search_prompt("coffee shops")
        assert result["search_type"] == "maps"
        assert result["keywords"] == "coffee shops"
        assert result["location"] == "Los Angeles"

    def test_parse_dork_with_gmail(self):
        result = parse_search_prompt("real estate @gmail.com")
        assert result["search_type"] == "dork"
        assert result["keywords"] == "real estate @gmail.com"

    def test_parse_dork_with_yahoo(self):
        result = parse_search_prompt("plumbers @yahoo.com Los Angeles")
        assert result["search_type"] == "dork"
        assert result["keywords"] == "plumbers @yahoo.com Los Angeles"

    def test_parse_dork_with_contact(self):
        result = parse_search_prompt("hotels contact@*.com")
        assert result["search_type"] == "dork"

    def test_parse_dork_with_site(self):
        result = parse_search_prompt("site:linkedin.com software engineer")
        assert result["search_type"] == "dork"


class TestProcessAndCleanData:
    def test_empty_data(self):
        result = process_and_clean_data([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_process_maps_data(self, sample_business_data):
        result = process_and_clean_data(sample_business_data)
        assert len(result) == 2
        assert "Business Name" in result.columns
        assert "Phone Number" in result.columns

    def test_remove_duplicates(self):
        data = [
            {"Business Name": "Test", "Phone Number": "123", "Website": "http://a.com"},
            {"Business Name": "Test", "Phone Number": "456", "Website": "http://b.com"},
        ]
        result = process_and_clean_data(data)
        assert len(result) == 1

    def test_filter_no_contacts(self):
        data = [
            {"Business Name": "No Contact Business", "Phone Number": "", "Website": ""},
            {
                "Business Name": "Valid Business",
                "Phone Number": "123-456",
                "Website": "",
            },
        ]
        result = process_and_clean_data(data)
        assert len(result) == 1
        assert result.iloc[0]["Business Name"] == "Valid Business"

    def test_filter_empty_business_names(self):
        data = [
            {"Business Name": "", "Phone Number": "123-456", "Website": "http://a.com"},
            {"Business Name": "Valid", "Phone Number": "", "Website": "http://b.com"},
        ]
        result = process_and_clean_data(data)
        assert len(result) == 1
        assert result.iloc[0]["Business Name"] == "Valid"

    def test_email_data_processing(self, sample_email_data):
        result = process_and_clean_data(sample_email_data)
        assert len(result) == 1
        assert "Email" in result.columns

    def test_invalid_phone_filtered(self):
        data = [
            {"Business Name": "Valid", "Phone Number": "123", "Website": ""},
            {
                "Business Name": "Also Valid",
                "Phone Number": "(555) 123-4567",
                "Website": "",
            },
        ]
        result = process_and_clean_data(data)
        assert len(result) == 1
        assert "(555) 123-4567" in result["Phone Number"].values


class TestRegexPatterns:
    def test_phone_regex_standard(self):
        from lead_scraper import PHONE_REGEX
        import re

        phones = [
            "(555) 123-4567",
            "555-123-4567",
            "555 123 4567",
            "+1-555-123-4567",
        ]
        for phone in phones:
            match = re.search(PHONE_REGEX, phone)
            assert match is not None, f"Failed to match: {phone}"

    def test_email_regex(self):
        from lead_scraper import EMAIL_REGEX
        import re

        emails = [
            "test@example.com",
            "user.name@domain.org",
            "contact@company.co.uk",
        ]
        for email in emails:
            match = re.search(EMAIL_REGEX, email)
            assert match is not None, f"Failed to match: {email}"
