# Google Drive Batch Sink


Description
-----------
Sink plugin saves files from the pipeline to Google Drive directory via Google Drive API.

Properties
----------

### Basic

**File Body Field:** Name of the schema field (should be BYTES type) which will be used as body of file.
The minimal input schema should contain only this field.

**File Name Field:** Name of the schema field (should be STRING type) which will be used as name of file. 
Is optional. In the case it is not set files have randomly generated 16-symbols names.

**File Mime Field:** Name of the schema field (should be STRING type) which will be used as MIME type of file. 
All MIME types are supported except [Google Drive types](https://developers.google.com/drive/api/v3/mime-types).
Is optional. In the case it is not set Google API will try to recognize file's MIME type automatically.

**Directory Identifier:** Identifier of the destination folder.

### Authentication

**Authentication Type:** Type of authentication used to access Google API. 
OAuth2 and Service account types are available.

#### OAuth2 Properties

**Client ID:** OAuth2 client id.

**Client Secret:** OAuth2 client secret.

**Refresh Token:** OAuth2 refresh token.

**Access Token:** OAuth2 access token.

#### Service Account Properties

**Account File Path:** Path on the local file system of the service account key used for authorization. 
Can be set to 'auto-detect' for getting service account from system variable.
The file/system variable must be present on every node in the cluster.
Service account json can be generated on Google Cloud 
[Service Account page](https://console.cloud.google.com/iam-admin/serviceaccounts)

* **File Path**: Path on the local file system of the service account key used for
  authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
  When running on other clusters, the file must be present on every node in the cluster.

* **JSON**: Contents of the service account JSON file.


### Retrying

**Max Retry Count:** Maximum number of retry attempts.

**Max Retry Wait:** Maximum wait time for attempt in seconds. Initial wait time is one second and it grows exponentially.

**Max Retry Jitter wait:** Maximum additional wait time in milliseconds.
