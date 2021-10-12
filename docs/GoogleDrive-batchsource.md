# Google Drive Batch Source


Description
-----------
Reads a fileset from specified Google Drive directory via Google Drive API.

Properties
----------
### Basic

**Directory Identifier:** Identifier of the source folder. This comes after “folders/” in the URL. For example, if 
the URL was “https://drive.google.com/drive/folders/1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g?resourcekey=0-XVijrJSp3E3gkdJp20MpCQ”,
then the Directory Identifier would be “1dyUEebJaFnWa3Z4n0BFMVAXQ7mfUH11g”.

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

**Authentication Type:** Type of authentication used to access Google API. Make sure Google Drive Api is enabled.
OAuth2 and Service account types are available.

#### OAuth2 Properties

OAuth2 client credentials can be generated on Google Cloud
[Credentials Page](https://console.cloud.google.com/apis/credentials)

**Client ID:** OAuth2 client id used to identify the application.

**Client Secret:** OAuth2 client secret used to access the authorization server.

**Refresh Token:** OAuth2 refresh token to acquire new access tokens.

For more details on OAuth2, see [Google Drive Api Documentation](https://developers.google.com/drive/api/v3/about-auth)

#### Service Account Properties

**Account File Path:** Path on the local file system of the service account key used for authorization. 
Can be set to 'auto-detect' for getting service account from system variable.
The file/system variable must be present on every node in the cluster.
Service account json can be generated on Google Cloud 
[Service Account page](https://console.cloud.google.com/iam-admin/serviceaccounts)

* **File Path**: Path on the local file system of the service account key used for
  authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
  When running on other clusters, the file must be present on every node in the cluster.
  When using 'auto-detect', the cluster needs to be configured with the scope required by the Google Drive Api,
  otherwise the preview as well as pipeline run will fail with insufficient permission error. Here, the required scope
  is "https://www.googleapis.com/auth/drive.readonly".

* **JSON**: Contents of the service account JSON file.

Make sure that the Google Drive Folder is shared to the service account email used. `Viewer` role must be granted to 
the specified service account to read files from the Google Drive Folder.

### Advanced

**Maximum Partition Size:** Maximum body size for each structured record specified in bytes. 
Default 0 value means unlimited. Is not applicable for files in Google formats.

**Body Output Format** Output format for body of file. "Bytes" and "String" values are available.

### Exporting

**Google Documents Export Format:** MIME type which is used for Google Documents when converted to structured records.

**Google Spreadsheets Export Format:** MIME type which is used for Google Spreadsheets when converted to structured records.

**Google Drawings Export Format:** MIME type which is used for Google Drawings when converted to structured records.

**Google Presentations Export Format:** MIME type which is used for Google Presentations when converted to structured records.