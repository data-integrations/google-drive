# Google Drive Batch Source


Description
-----------
Reads a fileset from specified Google Drive directory via Google Drive API.

Properties
----------
### Basic

**Directory Identifier:** Identifier of the source folder.

**File Metadata Properties:** Properties that represent metadata of files. 
They will be a part of output structured record. Descriptions for properties can be view at 
[Drive API file reference](https://developers.google.com/drive/api/v3/reference/files).

### Filtering

**Filter:** Filter that can be applied to the files in the selected directory. 
Filters follow the [Google Drive filters syntax](https://developers.google.com/drive/api/v3/ref-search-terms).

**Modification Date Range:** Filter that narrows set of files by modified date range. 
User can select either among predefined or custom entered ranges. 
For _Custom_ selection the dates range can be specified via **Start Date** and **End Date**. 

**Start Date:** Start date for custom modification date range. 
Is shown only when _Custom_ range is selected for **Modification Rate Range** field. 
[RFC3339](https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**End Date:** End date for custom modification date range. 
Is shown only when _Custom_ range is selected for **Modification Date Range** field.
[RFC3339](https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.

**File Types To Pull:** Types of files which should be pulled from a specified directory. 
The following values are supported: binary (all non-Google Drive formats), Google Documents, Google Spreadsheets, 
Google Drawings, Google Presentations and Google Apps Scripts. 
For Google Drive formats user should specify exporting format in **Exporting** section.

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

**Max Retry Jitter Wait:** Maximum additional wait time in milliseconds.

### Advanced

**Maximum Partition Size:** Maximum body size for each structured record specified in bytes. 
Default 0 value means unlimited. Is not applicable for files in Google formats.

**Body Output Format** Output format for body of file. "Bytes" and "String" values are available.

### Exporting

**Google Documents Export Format:** MIME type which is used for Google Documents when converted to structured records.

**Google Spreadsheets Export Format:** MIME type which is used for Google Spreadsheets when converted to structured records.

**Google Drawings Export Format:** MIME type which is used for Google Drawings when converted to structured records.

**Google Presentations Export Format:** MIME type which is used for Google Presentations when converted to structured records.