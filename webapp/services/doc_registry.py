"""
📚 Connector Documentation Registry
Known official documentation URLs for popular data connectors.

This registry enables automatic pre-crawling of official docs when a connector is created.
Uses a two-gate URL filtering system:
- Gate 1 (Hard): url_patterns (include) and exclude_patterns (deny)
- Gate 2 (Soft): Keyword scoring for ranking
"""

from typing import Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class ConnectorDocConfig:
    """
    Configuration for a connector's official documentation.
    
    URL Pattern Matching Rules:
    - If url_patterns is set, ONLY URLs matching at least one pattern are allowed
    - If exclude_patterns is set, matching URLs are rejected (after include check)
    - Patterns use glob-style matching (fnmatch): * matches any sequence, ? matches single char
    - Patterns should NOT overlap/contradict each other
    """
    name: str
    official_docs: List[str]
    domain: str
    # Gate 1: Hard pattern filters (glob-style)
    url_patterns: List[str] = field(default_factory=list)  # Include patterns (if set, ONLY these are allowed)
    exclude_patterns: List[str] = field(default_factory=list)  # Exclude patterns (always rejected)
    # Specialized documentation URLs
    api_reference: Optional[str] = None
    auth_docs: Optional[str] = None
    rate_limit_docs: Optional[str] = None
    webhooks_docs: Optional[str] = None
    sdk_docs: Optional[str] = None
    changelog: Optional[str] = None
    # Additional extraction-specific URLs
    graphql_objects: Optional[str] = None  # URL listing all GraphQL objects/types
    rest_resources: Optional[str] = None   # URL listing all REST resources
    bulk_operations: Optional[str] = None  # Bulk/batch API docs
    pagination_docs: Optional[str] = None  # Pagination documentation


# Registry of known connector documentation
# Add new connectors here as they are researched
CONNECTOR_DOC_REGISTRY: Dict[str, ConnectorDocConfig] = {
    # E-commerce Platforms
    "shopify": ConnectorDocConfig(
        name="Shopify",
        official_docs=[
            # API Overview - lists all extraction capabilities
            "https://shopify.dev/docs/api",
            # REST API - all resources/objects
            "https://shopify.dev/docs/api/admin-rest",
            "https://shopify.dev/docs/api/admin-rest/latest/resources",
            # GraphQL API - all objects
            "https://shopify.dev/docs/api/admin-graphql",
            "https://shopify.dev/docs/api/admin-graphql/latest/objects",
            # Authentication - OAuth, session tokens, etc.
            "https://shopify.dev/docs/apps/build/authentication-authorization",
            # Webhooks
            "https://shopify.dev/docs/api/admin-rest/latest/resources/webhook",
            # Rate limits
            "https://shopify.dev/docs/api/usage/rate-limits",
            # Pagination
            "https://shopify.dev/docs/api/usage/pagination-graphql",
            "https://shopify.dev/docs/api/usage/pagination-rest",
            # Bulk operations (GraphQL)
            "https://shopify.dev/docs/api/usage/bulk-operations",
        ],
        domain="shopify.dev",
        # Gate 1: API-only patterns (clean separation, no contradictions)
        url_patterns=[
            "/docs/api",
            "/docs/api/*",
            "/docs/api/admin-rest/*",
            "/docs/api/admin-graphql/*",
            "/docs/api/storefront/*",
            "/docs/api/usage/*",
            "/docs/apps/build/authentication-authorization/*",
            "/changelog",
            "/changelog/*",
        ],
        exclude_patterns=[
            "/docs/api/shipping-partner-platform/*",  # Separate product
            "*/beta/*",
            "*/deprecated/*",
        ],
        api_reference="https://shopify.dev/docs/api/admin-rest/latest/resources",
        auth_docs="https://shopify.dev/docs/apps/build/authentication-authorization",
        rate_limit_docs="https://shopify.dev/docs/api/usage/rate-limits",
        webhooks_docs="https://shopify.dev/docs/api/admin-rest/latest/resources/webhook",
        changelog="https://shopify.dev/changelog",
        # Additional critical URLs for data extraction research
        graphql_objects="https://shopify.dev/docs/api/admin-graphql/latest/objects",
        rest_resources="https://shopify.dev/docs/api/admin-rest/latest/resources",
        bulk_operations="https://shopify.dev/docs/api/usage/bulk-operations",
        pagination_docs="https://shopify.dev/docs/api/usage/pagination-graphql",
    ),
    "woocommerce": ConnectorDocConfig(
        name="WooCommerce",
        official_docs=[
            "https://woocommerce.github.io/woocommerce-rest-api-docs/",
            "https://developer.woocommerce.com/docs/",
        ],
        domain="woocommerce.github.io",
        api_reference="https://woocommerce.github.io/woocommerce-rest-api-docs/",
        auth_docs="https://woocommerce.github.io/woocommerce-rest-api-docs/#authentication"
    ),
    "bigcommerce": ConnectorDocConfig(
        name="BigCommerce",
        official_docs=[
            "https://developer.bigcommerce.com/docs/rest-management",
            "https://developer.bigcommerce.com/docs/rest-catalog",
        ],
        domain="developer.bigcommerce.com",
        api_reference="https://developer.bigcommerce.com/docs/rest-management",
        auth_docs="https://developer.bigcommerce.com/docs/start/authentication"
    ),
    "magento": ConnectorDocConfig(
        name="Magento/Adobe Commerce",
        official_docs=[
            "https://developer.adobe.com/commerce/webapi/rest/",
            "https://developer.adobe.com/commerce/webapi/graphql/",
        ],
        domain="developer.adobe.com",
        api_reference="https://developer.adobe.com/commerce/webapi/rest/quick-reference/"
    ),
    
    # Social Media & Advertising
    "facebook": ConnectorDocConfig(
        name="Facebook",
        official_docs=[
            "https://developers.facebook.com/docs/graph-api",
            "https://developers.facebook.com/docs/graph-api/reference",
        ],
        domain="developers.facebook.com",
        api_reference="https://developers.facebook.com/docs/graph-api/reference",
        auth_docs="https://developers.facebook.com/docs/facebook-login/",
        rate_limit_docs="https://developers.facebook.com/docs/graph-api/overview/rate-limiting"
    ),
    "facebook_ads": ConnectorDocConfig(
        name="Facebook Ads",
        official_docs=[
            "https://developers.facebook.com/docs/marketing-api",
            "https://developers.facebook.com/docs/marketing-api/insights",
        ],
        domain="developers.facebook.com",
        api_reference="https://developers.facebook.com/docs/marketing-api/reference",
        auth_docs="https://developers.facebook.com/docs/marketing-api/overview/authentication"
    ),
    "instagram": ConnectorDocConfig(
        name="Instagram",
        official_docs=[
            "https://developers.facebook.com/docs/instagram-api",
            "https://developers.facebook.com/docs/instagram-basic-display-api",
        ],
        domain="developers.facebook.com",
        api_reference="https://developers.facebook.com/docs/instagram-api/reference"
    ),
    "twitter": ConnectorDocConfig(
        name="Twitter/X",
        official_docs=[
            "https://developer.twitter.com/en/docs/twitter-api",
            "https://developer.twitter.com/en/docs/twitter-api/tweets/lookup/api-reference",
        ],
        domain="developer.twitter.com",
        api_reference="https://developer.twitter.com/en/docs/api-reference-index",
        auth_docs="https://developer.twitter.com/en/docs/authentication/overview",
        rate_limit_docs="https://developer.twitter.com/en/docs/twitter-api/rate-limits"
    ),
    "linkedin": ConnectorDocConfig(
        name="LinkedIn",
        official_docs=[
            "https://learn.microsoft.com/en-us/linkedin/",
            "https://learn.microsoft.com/en-us/linkedin/marketing/",
        ],
        domain="learn.microsoft.com",
        api_reference="https://learn.microsoft.com/en-us/linkedin/shared/references/v2/",
        auth_docs="https://learn.microsoft.com/en-us/linkedin/shared/authentication/authentication"
    ),
    "google_ads": ConnectorDocConfig(
        name="Google Ads",
        official_docs=[
            "https://developers.google.com/google-ads/api/docs/start",
            "https://developers.google.com/google-ads/api/reference/rpc",
        ],
        domain="developers.google.com",
        api_reference="https://developers.google.com/google-ads/api/reference/rpc",
        auth_docs="https://developers.google.com/google-ads/api/docs/oauth/overview"
    ),
    "tiktok_ads": ConnectorDocConfig(
        name="TikTok Ads",
        official_docs=[
            "https://business-api.tiktok.com/portal/docs",
            "https://business-api.tiktok.com/portal/docs?rid=5ipocbxyw8u",
        ],
        domain="business-api.tiktok.com",
        api_reference="https://business-api.tiktok.com/portal/docs?rid=5ipocbxyw8u"
    ),
    
    # CRM & Sales
    "salesforce": ConnectorDocConfig(
        name="Salesforce",
        official_docs=[
            "https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_what_is_rest_api.htm",
            "https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/",
        ],
        domain="developer.salesforce.com",
        api_reference="https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/",
        auth_docs="https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_oauth_and_connected_apps.htm",
        rate_limit_docs="https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_api.htm"
    ),
    "hubspot": ConnectorDocConfig(
        name="HubSpot",
        official_docs=[
            "https://developers.hubspot.com/docs/api/overview",
            "https://developers.hubspot.com/docs/api/crm/contacts",
        ],
        domain="developers.hubspot.com",
        api_reference="https://developers.hubspot.com/docs/api/crm/contacts",
        auth_docs="https://developers.hubspot.com/docs/api/oauth-quickstart-guide",
        rate_limit_docs="https://developers.hubspot.com/docs/api/usage-details"
    ),
    "pipedrive": ConnectorDocConfig(
        name="Pipedrive",
        official_docs=[
            "https://developers.pipedrive.com/docs/api/v1",
        ],
        domain="developers.pipedrive.com",
        api_reference="https://developers.pipedrive.com/docs/api/v1",
        auth_docs="https://developers.pipedrive.com/docs/api/v1/Authentication"
    ),
    "zoho_crm": ConnectorDocConfig(
        name="Zoho CRM",
        official_docs=[
            "https://www.zoho.com/crm/developer/docs/api/v6/",
        ],
        domain="zoho.com",
        api_reference="https://www.zoho.com/crm/developer/docs/api/v6/",
        auth_docs="https://www.zoho.com/crm/developer/docs/api/v6/oauth-overview.html"
    ),
    
    # Databases & Data Warehouses
    "mysql": ConnectorDocConfig(
        name="MySQL",
        official_docs=[
            "https://dev.mysql.com/doc/",
            "https://dev.mysql.com/doc/connector-j/en/",
        ],
        domain="dev.mysql.com",
        api_reference="https://dev.mysql.com/doc/refman/8.0/en/"
    ),
    "postgresql": ConnectorDocConfig(
        name="PostgreSQL",
        official_docs=[
            "https://www.postgresql.org/docs/current/",
            "https://jdbc.postgresql.org/documentation/",
        ],
        domain="postgresql.org"
    ),
    "mongodb": ConnectorDocConfig(
        name="MongoDB",
        official_docs=[
            "https://www.mongodb.com/docs/manual/",
            "https://www.mongodb.com/docs/drivers/java/sync/current/",
        ],
        domain="mongodb.com",
        api_reference="https://www.mongodb.com/docs/manual/reference/"
    ),
    "snowflake": ConnectorDocConfig(
        name="Snowflake",
        official_docs=[
            "https://docs.snowflake.com/en/developer",
            "https://docs.snowflake.com/en/sql-reference",
        ],
        domain="docs.snowflake.com",
        api_reference="https://docs.snowflake.com/en/developer-guide/sql-api/reference"
    ),
    "bigquery": ConnectorDocConfig(
        name="Google BigQuery",
        official_docs=[
            "https://cloud.google.com/bigquery/docs",
            "https://cloud.google.com/bigquery/docs/reference/rest",
        ],
        domain="cloud.google.com",
        api_reference="https://cloud.google.com/bigquery/docs/reference/rest"
    ),
    
    # Cloud Storage
    "s3": ConnectorDocConfig(
        name="Amazon S3",
        official_docs=[
            "https://docs.aws.amazon.com/s3/",
            "https://docs.aws.amazon.com/AmazonS3/latest/API/",
        ],
        domain="docs.aws.amazon.com",
        api_reference="https://docs.aws.amazon.com/AmazonS3/latest/API/"
    ),
    "gcs": ConnectorDocConfig(
        name="Google Cloud Storage",
        official_docs=[
            "https://cloud.google.com/storage/docs",
            "https://cloud.google.com/storage/docs/json_api",
        ],
        domain="cloud.google.com",
        api_reference="https://cloud.google.com/storage/docs/json_api/v1"
    ),
    "azure_blob": ConnectorDocConfig(
        name="Azure Blob Storage",
        official_docs=[
            "https://learn.microsoft.com/en-us/azure/storage/blobs/",
            "https://learn.microsoft.com/en-us/rest/api/storageservices/",
        ],
        domain="learn.microsoft.com",
        api_reference="https://learn.microsoft.com/en-us/rest/api/storageservices/"
    ),
    
    # Payment & Finance
    "stripe": ConnectorDocConfig(
        name="Stripe",
        official_docs=[
            "https://stripe.com/docs/api",
            "https://stripe.com/docs/api/charges",
        ],
        domain="stripe.com",
        api_reference="https://stripe.com/docs/api",
        auth_docs="https://stripe.com/docs/api/authentication",
        rate_limit_docs="https://stripe.com/docs/rate-limits"
    ),
    "square": ConnectorDocConfig(
        name="Square",
        official_docs=[
            "https://developer.squareup.com/docs/",
            "https://developer.squareup.com/reference/square",
        ],
        domain="developer.squareup.com",
        api_reference="https://developer.squareup.com/reference/square"
    ),
    "paypal": ConnectorDocConfig(
        name="PayPal",
        official_docs=[
            "https://developer.paypal.com/docs/api/overview/",
            "https://developer.paypal.com/docs/api/orders/v2/",
        ],
        domain="developer.paypal.com",
        api_reference="https://developer.paypal.com/docs/api/reference/"
    ),
    
    # Communication & Support
    "zendesk": ConnectorDocConfig(
        name="Zendesk",
        official_docs=[
            "https://developer.zendesk.com/api-reference/",
            "https://developer.zendesk.com/api-reference/ticketing/introduction/",
        ],
        domain="developer.zendesk.com",
        api_reference="https://developer.zendesk.com/api-reference/",
        auth_docs="https://developer.zendesk.com/documentation/ticketing/getting-started/getting-started-with-the-zendesk-api/",
        rate_limit_docs="https://developer.zendesk.com/api-reference/introduction/rate-limits/"
    ),
    "intercom": ConnectorDocConfig(
        name="Intercom",
        official_docs=[
            "https://developers.intercom.com/docs/",
            "https://developers.intercom.com/docs/references/rest-api/api.intercom.io/",
        ],
        domain="developers.intercom.com",
        api_reference="https://developers.intercom.com/docs/references/rest-api/api.intercom.io/"
    ),
    "twilio": ConnectorDocConfig(
        name="Twilio",
        official_docs=[
            "https://www.twilio.com/docs/usage/api",
            "https://www.twilio.com/docs/sms/api",
        ],
        domain="twilio.com",
        api_reference="https://www.twilio.com/docs/usage/api"
    ),
    "sendgrid": ConnectorDocConfig(
        name="SendGrid",
        official_docs=[
            "https://docs.sendgrid.com/api-reference/",
            "https://docs.sendgrid.com/for-developers/sending-email/api-getting-started",
        ],
        domain="docs.sendgrid.com",
        api_reference="https://docs.sendgrid.com/api-reference/"
    ),
    
    # Project Management
    "jira": ConnectorDocConfig(
        name="Jira",
        official_docs=[
            "https://developer.atlassian.com/cloud/jira/platform/rest/v3/",
            "https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/",
        ],
        domain="developer.atlassian.com",
        api_reference="https://developer.atlassian.com/cloud/jira/platform/rest/v3/",
        auth_docs="https://developer.atlassian.com/cloud/jira/platform/oauth-2-3lo-apps/",
        rate_limit_docs="https://developer.atlassian.com/cloud/jira/platform/rate-limiting/"
    ),
    "asana": ConnectorDocConfig(
        name="Asana",
        official_docs=[
            "https://developers.asana.com/docs/",
            "https://developers.asana.com/reference/rest-api-reference",
        ],
        domain="developers.asana.com",
        api_reference="https://developers.asana.com/reference/rest-api-reference"
    ),
    "monday": ConnectorDocConfig(
        name="Monday.com",
        official_docs=[
            "https://developer.monday.com/api-reference/docs/",
            "https://developer.monday.com/api-reference/reference/items",
        ],
        domain="developer.monday.com",
        api_reference="https://developer.monday.com/api-reference/reference/"
    ),
    "notion": ConnectorDocConfig(
        name="Notion",
        official_docs=[
            "https://developers.notion.com/docs/getting-started",
            "https://developers.notion.com/reference/intro",
        ],
        domain="developers.notion.com",
        api_reference="https://developers.notion.com/reference/intro"
    ),
    
    # Analytics
    "google_analytics": ConnectorDocConfig(
        name="Google Analytics",
        official_docs=[
            "https://developers.google.com/analytics/devguides/reporting/data/v1",
            "https://developers.google.com/analytics/devguides/config/admin/v1",
        ],
        domain="developers.google.com",
        api_reference="https://developers.google.com/analytics/devguides/reporting/data/v1/rest"
    ),
    "mixpanel": ConnectorDocConfig(
        name="Mixpanel",
        official_docs=[
            "https://developer.mixpanel.com/reference/overview",
            "https://developer.mixpanel.com/reference/raw-data-export-api",
        ],
        domain="developer.mixpanel.com",
        api_reference="https://developer.mixpanel.com/reference/overview"
    ),
    "amplitude": ConnectorDocConfig(
        name="Amplitude",
        official_docs=[
            "https://www.docs.developers.amplitude.com/",
            "https://www.docs.developers.amplitude.com/analytics/apis/",
        ],
        domain="docs.developers.amplitude.com",
        api_reference="https://www.docs.developers.amplitude.com/analytics/apis/"
    ),
    "segment": ConnectorDocConfig(
        name="Segment",
        official_docs=[
            "https://segment.com/docs/connections/sources/",
            "https://segment.com/docs/connections/spec/",
        ],
        domain="segment.com",
        api_reference="https://segment.com/docs/api/"
    ),
}


def get_connector_docs(connector_name: str) -> Optional[ConnectorDocConfig]:
    """
    Get documentation configuration for a connector.
    
    Args:
        connector_name: Name of the connector (case-insensitive)
        
    Returns:
        ConnectorDocConfig if found, None otherwise
    """
    # Normalize the name (lowercase, remove spaces/hyphens)
    normalized = connector_name.lower().replace(" ", "_").replace("-", "_")
    
    # Direct lookup
    if normalized in CONNECTOR_DOC_REGISTRY:
        return CONNECTOR_DOC_REGISTRY[normalized]
    
    # Try partial matches
    for key, config in CONNECTOR_DOC_REGISTRY.items():
        if key in normalized or normalized in key:
            return config
        if config.name.lower() in connector_name.lower():
            return config
    
    return None


def get_all_connector_names() -> List[str]:
    """Get list of all registered connector names."""
    return [config.name for config in CONNECTOR_DOC_REGISTRY.values()]


def get_official_doc_urls(connector_name: str) -> List[str]:
    """
    Get all official documentation URLs for a connector.
    
    Args:
        connector_name: Name of the connector
        
    Returns:
        List of documentation URLs (empty if not found)
    """
    config = get_connector_docs(connector_name)
    if not config:
        return []
    
    urls = list(config.official_docs)
    
    # Add specialized docs if available
    if config.api_reference and config.api_reference not in urls:
        urls.append(config.api_reference)
    if config.auth_docs and config.auth_docs not in urls:
        urls.append(config.auth_docs)
    if config.rate_limit_docs and config.rate_limit_docs not in urls:
        urls.append(config.rate_limit_docs)
    
    return urls


def get_connector_domain(connector_name: str) -> Optional[str]:
    """Get the official documentation domain for a connector."""
    config = get_connector_docs(connector_name)
    return config.domain if config else None
