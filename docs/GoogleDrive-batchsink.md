# Google Drive Batch Sink


Description
-----------
Sink plugin saves files from the pipeline to Google Drive directory via Google Drive API.

Properties
----------

### Basic

**File body field:** Name of the schema field (should be BYTES type) which will be used as body of file.
The minimal input schema should contain only this field.

**File name field:** Name of the schema field (should be STRING type) which will be used as name of file. 
Is optional. In the case it is not set files have randomly generated 16-symbols names.

**File mime field:** Name of the schema field (should be STRING type) which will be used as MIME type of file. 
All MIME types are supported except [Google Drive types](https://developers.google.com/drive/api/v3/mime-types).
Is optional. In the case it is not set Google API will try to recognize file's MIME type automatically.

**Directory identifier:** Identifier of the destination folder.

### Authentication

**Authentication type:** Type of authentication used to access Google API. 
OAuth2 and Service account types are available. Default is OAuth2.

**Client ID:** OAuth2 client id. Is shown only when _OAuth2_ auth type is selected for **Authentication type** property.

**Client secret:** OAuth2 client secret. 
Is shown only when _OAuth2_ auth type is selected for **Authentication type** property.

**Refresh token:** OAuth2 refresh token. 
Is shown only when _OAuth2_ auth type is selected for **Authentication type** property.

**Access token:** OAuth2 access token. 
Is shown only when _OAuth2_ auth type is selected for **Authentication type** property.

**Account file path:** Path on the local file system of the service account key used for authorization. 
Is shown only when _Service account_ auth type is selected for **Authentication type** property.
Can be set to 'auto-detect' when running on a Dataproc cluster. 
When running on other clusters, the file must be present on every node in the cluster.
Service account json can be generated on Google Cloud 
[Service Account page](https://console.cloud.google.com/iam-admin/serviceaccounts)