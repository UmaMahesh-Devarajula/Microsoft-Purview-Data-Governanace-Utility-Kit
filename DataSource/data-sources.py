COMMON_PROPERTIES = ["ds_name", "collection_name"]

PARSED_FIELDS = ["subscription_id", "resource_group", "resource_name"]

SOURCE_TYPES = {
    "AzureSubscription": {
        "kind": "AzureSubscription",
        "properties": ["subscription_id", "resource_id"]
    },
    "AzureResourceGroup": {
        "kind": "AzureResourceGroup",
        "properties": ["resource_group", "subscription_id", "resource_id"]
    },
    "AWS account": {
        "kind": "AmazonAccount",
        "properties": ["awsAccount_id"]
    },
    "AzureSynapseAnalytics": {
        "kind": "AzureSynapseWorkspace",
        "properties": ["dedicated_SqlEndpoint", "serverless_SqlEndpoint", "resource_id", "location"]
    },
    "AmazonPostgreSql": {
        "kind": "AmazonPostgreSql",
        "properties": ["server_Endpoint", "Port"]
    },
    "AmazonSql": {
        "kind": "AmazonSql",
        "properties": ["server_Endpoint", "Port"]
    },
    "AmazonRedShift": {
        "kind": "AmazonRedShift",
        "properties": ["Host", "Port"]
    },
    "AmazonS3": {
        "kind": "AmazonS3",
        "properties": ["service_Url"]
    },
    "AdlsGen2": {
        "kind": "AdlsGen2",
        "properties": ["endpoint", "resource_id", "location"]
    },
    "AzureStorage": {
        "kind": "AzureStorage",
        "properties": ["endpoint", "resource_id", "location"]
    },
    "AzureSqlDatabase": {
        "kind": "AzureSqlDatabase",
        "properties": ["server_endpoint", "resource_id", "location"]
    },
    "AzureCosmosDb": {
        "kind": "AzureCosmosDb",
        "properties": ["account_uri", "resource_id", "location"]
    },
    "Fabric": {
        "kind": "Fabric",
        "properties": ["tenant"]
    },
    "SqlServer": {
        "kind": "SqlServer",
        "properties": ["server_endpoint"]
    },
    "Oracle": {
        "kind": "Oracle",
        "properties": ["host", "port", "service_name"]
    },
    "Teradata": {
        "kind": "Teradata",
        "properties": ["host"]
    },
    "SapS4Hana": {
        "kind": "SapS4Hana",
        "properties": ["application_server", "system_number"]
    }
}